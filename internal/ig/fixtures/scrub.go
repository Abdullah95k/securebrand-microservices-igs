// Package fixtures scrubs logged-out Instagram responses into parser
// fixtures that hold no personal data (brief §6, M5): usernames and names
// pseudonymised consistently, people's ids replaced, the text people wrote
// blanked to the same UTF-16 length, shortcodes recomputed from the
// pseudonymised media pks (which keep the time bits the parser decodes),
// and signed CDN URLs reduced to their file name and expiry.
package fixtures

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf16"
)

// Version is recorded in fixture metadata; raise it when Scrub learns a new
// pattern.
const Version = 1

var (
	// Keys whose string is a username: pseudonymised, and replaced wherever
	// else it appears (an "Original audio - <username>" title).
	usernameKeys = map[string]bool{"username": true, "owner_username": true}
	// Keys whose string names a person or a place: pseudonymised.
	nameKeys = map[string]bool{"full_name": true, "name": true}
	// Keys whose string a person wrote or that describes them: blanked.
	textKeys = map[string]bool{"text": true, "biography": true, "accessibility_caption": true, "alt_text": true,
		"title": true, "original_audio_title": true, "display_artist": true, "description": true, "subtitle": true}
	// Keys whose value is an id of a person or an item.
	idKeys = map[string]bool{"pk": true, "id": true, "user_id": true, "owner_id": true, "pk_id": true, "fbid_v2": true,
		"strong_id__": true, "media_id": true, "comment_id": true, "parent_comment_id": true, "audio_asset_id": true,
		"audio_cluster_id": true, "reel_id": true, "initial_reel_id": true, "page_id": true}
	// Keys whose string is a link outside Instagram's CDN.
	linkKeys = map[string]bool{"url": true, "lynx_url": true, "external_url": true, "seo_canonical_url": true, "link": true}
	// Topic names, and the error and extension objects of an answer, are
	// Instagram's, not people's: kept as they are.
	keepKeys = map[string]bool{"topic_name": true, "__typename": true, "product_type": true, "media_type": true,
		"errors": true, "extensions": true}

	cdnURL     = regexp.MustCompile(`https?:(?:\\*/){2}[a-z0-9.\-]*(?:cdninstagram\.com|fbcdn\.net)[^"'\s<>\\]*`)
	digitsRe   = regexp.MustCompile(`\d{6,}`)
	jsonScript = regexp.MustCompile(`(?s)<script type="application/json"([^>]*)>(.*?)</script>`)
	codeURL    = regexp.MustCompile(`/(p|reel|tv)/([A-Za-z0-9_\-]{5,})`)
)

// Scrubber holds the pseudonyms of one fixture set, so the same person or
// post gets the same pseudonym across files.
type Scrubber struct {
	Salt      string
	usernames map[string]string
	names     map[string]string
	ids       map[string]string
	codes     map[string]string
	// Originals are the personal values seen, for Leaks.
	originals map[string]bool
}

// New returns a scrubber; salt keeps pseudonyms unguessable from outside.
func New(salt string) *Scrubber {
	return &Scrubber{Salt: salt, usernames: map[string]string{}, names: map[string]string{}, ids: map[string]string{},
		codes: map[string]string{}, originals: map[string]bool{}}
}

func (s *Scrubber) hash(kind, v string) uint64 {
	h := sha256.Sum256([]byte(s.Salt + "|" + kind + "|" + v))
	return binary.BigEndian.Uint64(h[:8])
}

// JSON scrubs a response of one or more JSON documents (one per line).
func (s *Scrubber) JSON(b []byte) ([]byte, error) {
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	var docs []any
	for {
		var v any
		if err := d.Decode(&v); err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("fixtures: decode: %w", err)
		}
		docs = append(docs, v)
	}
	for _, v := range docs {
		s.collect(v, "")
	}
	var out bytes.Buffer
	for _, v := range docs {
		enc, err := encode(s.transform(v, ""))
		if err != nil {
			return nil, err
		}
		out.Write(enc)
		out.WriteByte('\n')
	}
	return []byte(s.finish(out.String())), nil
}

// HTML keeps a page's embedded JSON payloads that hold one of the keys
// wanted (all when none), scrubbed, in a minimal page: the markup, scripts
// and meta tags (which repeat names and captions) are dropped.
func (s *Scrubber) HTML(b []byte, wanted ...string) ([]byte, error) {
	var keep [][]any
	var attrs []string
	for _, m := range jsonScript.FindAllSubmatch(b, -1) {
		body := m[2]
		if len(wanted) > 0 {
			hit := false
			for _, w := range wanted {
				if bytes.Contains(body, []byte(`"`+w+`"`)) {
					hit = true
				}
			}
			if !hit {
				continue
			}
		}
		d := json.NewDecoder(bytes.NewReader(body))
		d.UseNumber()
		var v any
		if err := d.Decode(&v); err != nil {
			continue
		}
		keep = append(keep, []any{v})
		attrs = append(attrs, string(m[1]))
	}
	for _, k := range keep {
		s.collect(k[0], "")
	}
	var out strings.Builder
	out.WriteString("<!DOCTYPE html><html><head><title>scrubbed</title></head><body>\n")
	for i, k := range keep {
		enc, err := encode(s.transform(k[0], ""))
		if err != nil {
			return nil, err
		}
		out.WriteString(`<script type="application/json"` + attrs[i] + `>`)
		out.Write(enc)
		out.WriteString("</script>\n")
	}
	out.WriteString("</body></html>\n")
	return []byte(s.finish(out.String())), nil
}

// collect records the pseudonyms of every id, username and name in v, and
// the shortcode of each media object (with its pk), before any is replaced.
func (s *Scrubber) collect(v any, key string) {
	if keepKeys[key] {
		return
	}
	switch x := v.(type) {
	case map[string]any:
		if code, ok := x["code"].(string); ok && code != "" {
			// A media object with its pk gets the shortcode of its new pk,
			// wherever the code was seen first.
			if pk := idString(x["pk"]); pk != "" {
				s.codes[code] = Shortcode(s.mediaPK(pk))
			} else if _, ok := s.codes[code]; !ok {
				s.codes[code] = s.pseudoCode(code)
			}
			s.originals[code] = true
		}
		for k, c := range x {
			s.collect(c, k)
		}
	case []any:
		for _, c := range x {
			s.collect(c, key)
		}
	case string:
		switch {
		case usernameKeys[key] && x != "":
			if _, ok := s.usernames[x]; !ok {
				s.usernames[x] = fmt.Sprintf("user_%08x", s.hash("user", strings.ToLower(x))&0xffffffff)
			}
			s.originals[x] = true
		case nameKeys[key] && x != "":
			if _, ok := s.names[x]; !ok {
				s.names[x] = fmt.Sprintf("Name %06x", s.hash("name", x)&0xffffff)
			}
			s.originals[x] = true
		case textKeys[key] && len([]rune(x)) >= 4:
			s.originals[x] = true
		case isIDKey(key):
			s.id(x)
		}
	case json.Number:
		if isIDKey(key) {
			s.id(x.String())
		}
	}
}

// id records the pseudonym of an id: a media pk ("pk" or "<pk>_<user pk>")
// keeps its time bits; any other id keeps its length.
func (s *Scrubber) id(v string) {
	for _, part := range strings.Split(v, "_") {
		if len(part) < 6 || !allDigits(part) {
			continue
		}
		if _, ok := s.ids[part]; ok {
			continue
		}
		s.ids[part] = s.mediaPK(part)
		s.originals[part] = true
	}
}

// mediaPK pseudonymises a numeric id. A media pk (its upper bits a time
// after 2010) keeps those bits and gets new lower 23 (shard and sequence),
// so the parser still decodes its time; another id gets a number of the
// same length.
func (s *Scrubber) mediaPK(v string) string {
	if p, ok := s.ids[v]; ok {
		return p
	}
	n, err := strconv.ParseUint(v, 10, 64)
	h := s.hash("id", v)
	if err == nil && len(v) >= 17 {
		ms := int64(n>>23) + 1314220021721
		if ms > 1262304000000 { // after 2010
			p := strconv.FormatUint((n>>23)<<23|h&(1<<23-1), 10)
			s.ids[v] = p
			return p
		}
	}
	lo := uint64(1)
	for range len(v) - 1 {
		lo *= 10
	}
	p := strconv.FormatUint(lo+h%(9*lo), 10)
	if len(v) == 1 {
		p = v
	}
	s.ids[v] = p
	return p
}

func (s *Scrubber) pseudoCode(code string) string {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	h := s.hash("code", code)
	b := make([]byte, len(code))
	for i := range b {
		b[i] = alphabet[(h>>(uint(i)%58))%64]
		h = h*6364136223846793005 + 1442695040888963407
	}
	return string(b)
}

// Shortcode encodes a media pk as Instagram's shortcode (base 64 over its
// URL alphabet).
func Shortcode(pk string) string {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	n, err := strconv.ParseUint(pk, 10, 64)
	if err != nil || n == 0 {
		return pk
	}
	var b []byte
	for n > 0 {
		b = append([]byte{alphabet[n%64]}, b...)
		n /= 64
	}
	return string(b)
}

func (s *Scrubber) transform(v any, key string) any {
	if keepKeys[key] {
		return v
	}
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, c := range x {
			out[k] = s.transform(c, k)
		}
		if code, ok := x["code"].(string); ok {
			out["code"] = s.codes[code]
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, c := range x {
			out[i] = s.transform(c, key)
		}
		return out
	case string:
		switch {
		case usernameKeys[key]:
			if p, ok := s.usernames[x]; ok {
				return p
			}
		case nameKeys[key]:
			if p, ok := s.names[x]; ok {
				return p
			}
		case textKeys[key]:
			return Blank(x)
		case linkKeys[key] && strings.HasPrefix(x, "http") && !cdnURL.MatchString(x):
			// A post's own permalink stays (its shortcode is replaced in
			// finish); anything else goes, l.instagram.com redirects
			// included: they wrap the external link (?u=https://t.me/...).
			if u, err := url.Parse(x); err == nil && u.Host == "www.instagram.com" && codeURL.MatchString(u.Path) {
				return x
			}
			return "https://example.com/link"
		case isIDKey(key):
			return s.replaceIDs(x)
		}
		return x
	case json.Number:
		if isIDKey(key) {
			return json.Number(s.replaceIDs(x.String()))
		}
	}
	return v
}

func (s *Scrubber) replaceIDs(v string) string {
	parts := strings.Split(v, "_")
	for i, p := range parts {
		if n, ok := s.ids[p]; ok {
			parts[i] = n
		}
	}
	return strings.Join(parts, "_")
}

// finish replaces what can sit inside any string: usernames and names
// (longest first), shortcodes in links, ids, and signed CDN URLs.
func (s *Scrubber) finish(out string) string {
	var pairs []string
	for _, m := range []map[string]string{s.usernames, s.names} {
		keys := make([]string, 0, len(m))
		for k := range m {
			if len(k) >= 3 {
				keys = append(keys, k)
			}
		}
		sort.Slice(keys, func(i, j int) bool { return len(keys[i]) > len(keys[j]) })
		for _, k := range keys {
			pairs = append(pairs, k, m[k])
		}
	}
	out = strings.NewReplacer(pairs...).Replace(out)
	// Usernames are case-insensitive: one may also appear capitalised in a
	// link or a caption.
	var alts []string
	for k := range s.usernames {
		if len(k) >= 4 {
			alts = append(alts, regexp.QuoteMeta(k))
		}
	}
	if len(alts) > 0 {
		sort.Slice(alts, func(i, j int) bool { return len(alts[i]) > len(alts[j]) })
		re := regexp.MustCompile(`(?i)` + strings.Join(alts, "|"))
		lower := map[string]string{}
		for k, v := range s.usernames {
			lower[strings.ToLower(k)] = v
		}
		out = re.ReplaceAllStringFunc(out, func(m string) string {
			if v, ok := lower[strings.ToLower(m)]; ok {
				return v
			}
			return m
		})
	}
	out = codeURL.ReplaceAllStringFunc(out, func(m string) string {
		g := codeURL.FindStringSubmatch(m)
		if c, ok := s.codes[g[2]]; ok {
			return "/" + g[1] + "/" + c
		}
		return m
	})
	out = digitsRe.ReplaceAllStringFunc(out, func(d string) string {
		if n, ok := s.ids[d]; ok {
			return n
		}
		return d
	})
	return cdnURL.ReplaceAllStringFunc(out, func(m string) string {
		clean := strings.ReplaceAll(m, `\/`, "/")
		u, err := url.Parse(clean)
		if err != nil {
			return "https://scontent.cdninstagram.com/scrubbed.jpg"
		}
		file := "/scrubbed.jpg"
		if i := strings.LastIndexByte(u.Path, '/'); i >= 0 && strings.Contains(u.Path[i:], ".") {
			ext := u.Path[strings.LastIndexByte(u.Path, '.'):]
			file = fmt.Sprintf("/%x%s", s.hash("file", u.Path)&0xffffffffff, ext)
		}
		q := ""
		if oe := u.Query().Get("oe"); oe != "" {
			q = "?oe=" + oe
		}
		res := "https://scontent.cdninstagram.com/v" + file + q
		if strings.Contains(m, `\/`) {
			res = strings.ReplaceAll(res, "/", `\/`)
		}
		return res
	})
}

// Leaks returns the personal values of the original that survive in
// scrubbed: usernames, names, text of 4 characters or more, people's ids
// and shortcodes.
func (s *Scrubber) Leaks(scrubbed []byte) []string {
	var out []string
	low := bytes.ToLower(scrubbed)
	for u := range s.usernames {
		if len(u) >= 4 && bytes.Contains(low, []byte(strings.ToLower(u))) {
			out = append(out, u)
		}
	}
	for v := range s.originals {
		if len(v) >= 4 && bytes.Contains(scrubbed, []byte(v)) {
			out = append(out, v)
		}
		if enc, _ := json.Marshal(v); len(v) >= 4 && bytes.Contains(scrubbed, enc[1:len(enc)-1]) {
			out = append(out, v)
		}
	}
	sort.Strings(out)
	return out
}

// Blank replaces letters and digits with x, keeping the length in UTF-16
// units, whitespace, punctuation and emoji: "@Ali #tag" becomes "@xxx #xxx".
func Blank(t string) string {
	var sb strings.Builder
	for _, r := range t {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsMark(r) {
			sb.WriteString(strings.Repeat("x", utf16.RuneLen(r)))
		} else {
			sb.WriteRune(r)
		}
	}
	return sb.String()
}

func encode(v any) ([]byte, error) {
	var buf bytes.Buffer
	e := json.NewEncoder(&buf)
	e.SetEscapeHTML(false)
	if err := e.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

func idString(v any) string {
	switch x := v.(type) {
	case string:
		return strings.SplitN(x, "_", 2)[0]
	case json.Number:
		return x.String()
	}
	return ""
}

// isIDKey reports keys whose values are ids: the known ones, and any
// "*_id" or "*_ids" (clips_tab_pinned_user_ids holds user ids).
func isIDKey(k string) bool {
	return idKeys[k] || strings.HasSuffix(k, "_id") || strings.HasSuffix(k, "_ids")
}

func allDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return s != ""
}
