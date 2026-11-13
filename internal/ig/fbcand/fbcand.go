// Package fbcand hands the facebook.com links on monitored accounts'
// profiles to the Facebook collector (D-065): POST <url>/v1/discovery/candidates
// with the link as found, once per link, and again only when the account's
// bio changes. The scheduler's leader runs it; it is off by default.
//
// Every scheduled account is one igs collects as a publisher, so each send
// names the profile the link was found on (evidence_url), except a private
// one's. The key is never logged.
package fbcand

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/net/publicsuffix"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/normalize"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
)

// Path is fbs's candidate endpoint.
const Path = "/v1/discovery/candidates"

// Results are fbs's answers; any other is counted as other.
var Results = map[string]bool{"queued": true, "on_list": true, "added": true, "public_waiting": true,
	"checked_recently": true, "ignored": true}

// batch is the most links one tick sends.
const batch = 20

// Sender finds and sends the links.
type Sender struct {
	DB   *pgxpool.Pool
	Cfg  config.IGFBCandidates
	HTTP *http.Client
	// Now is the clock (tests); time.Now when nil.
	Now func() time.Time

	since       time.Time // profiles read after this are scanned next
	pausedUntil time.Time
}

func (s *Sender) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// Tick records the links on profiles read since the last tick (all of them
// on the first), then sends the pending ones, within the day's ceiling.
func (s *Sender) Tick(ctx context.Context) error {
	if err := s.scan(ctx); err != nil {
		return err
	}
	if s.now().Before(s.pausedUntil) {
		return nil
	}
	return s.send(ctx)
}

func (s *Sender) scan(ctx context.Context) error {
	rows, err := s.DB.Query(ctx, `
		SELECT p.user_pk, coalesce(p.biography, ''), coalesce(p.bio_links::text, ''), coalesce(p.external_url, ''),
			coalesce(p.linked_facebook_page_id, ''), p.last_profile_read_at
		FROM profiles p
		WHERE p.status <> 'suppressed' AND p.last_profile_read_at > $1
			AND EXISTS (SELECT 1 FROM profile_schedule s WHERE s.user_pk = p.user_pk AND s.enabled)
		ORDER BY p.last_profile_read_at`, s.since)
	if err != nil {
		return err
	}
	type found struct {
		pk    int64
		links []string
		hash  string
		read  time.Time
	}
	var all []found
	for rows.Next() {
		var f found
		var bio, bioLinks, website, page string
		if err := rows.Scan(&f.pk, &bio, &bioLinks, &website, &page, &f.read); err != nil {
			rows.Close()
			return err
		}
		f.links = Links(bio, bioLinks, website, page)
		sum := sha256.Sum256([]byte(bio + "\x00" + bioLinks + "\x00" + website + "\x00" + page))
		f.hash = hex.EncodeToString(sum[:])
		all = append(all, f)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, f := range all {
		for _, ref := range f.links {
			// A link new to igs is pending; a known one again only when
			// the account it was found on changed its bio.
			if _, err := s.DB.Exec(ctx, `
				INSERT INTO fb_candidates (key, ref, user_pk, bio_hash) VALUES ($1, $2, $3, $4)
				ON CONFLICT (key) DO UPDATE SET ref = EXCLUDED.ref, bio_hash = EXCLUDED.bio_hash, pending = true
				WHERE fb_candidates.user_pk = EXCLUDED.user_pk AND fb_candidates.bio_hash <> EXCLUDED.bio_hash`,
				Key(ref), ref, f.pk, f.hash); err != nil {
				return err
			}
		}
		s.since = f.read
	}
	return nil
}

func (s *Sender) send(ctx context.Context) error {
	var today int
	day := s.now().UTC().Truncate(24 * time.Hour)
	if err := s.DB.QueryRow(ctx, `SELECT count(*) FROM fb_candidates WHERE sent_at >= $1`, day).Scan(&today); err != nil {
		return err
	}
	n := min(s.Cfg.PerDay-today, batch)
	if n <= 0 {
		return nil
	}
	rows, err := s.DB.Query(ctx, `
		SELECT c.key, c.ref, coalesce(p.username, ''), coalesce(p.private, false)
		FROM fb_candidates c LEFT JOIN profiles p ON p.user_pk = c.user_pk
		WHERE c.pending ORDER BY c.found_at LIMIT $1`, n)
	if err != nil {
		return err
	}
	type due struct{ key, ref, evidence string }
	var list []due
	for rows.Next() {
		var d due
		var username string
		var private bool
		if err := rows.Scan(&d.key, &d.ref, &username, &private); err != nil {
			rows.Close()
			return err
		}
		if !private {
			d.evidence = normalize.ProfileURL(username)
		}
		list = append(list, d)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, d := range list {
		result, final := s.post(ctx, d.ref, d.evidence)
		metrics.FBCandidates.WithLabelValues(result).Inc()
		if !final {
			// fbs is off, refuses the key, or can't be reached: the link
			// stays pending, and sending waits.
			s.pausedUntil = s.now().Add(s.Cfg.Backoff)
			slog.Warn("fb candidates: sending paused", "result", result, "for", s.Cfg.Backoff)
			return nil
		}
		if _, err := s.DB.Exec(ctx, `UPDATE fb_candidates SET pending = false, sent_at = $4, sends = sends + 1, result = $2
			WHERE key = $1 AND ref = $3`, d.key, result, d.ref, s.now()); err != nil {
			return err
		}
	}
	return nil
}

// post sends one link. final is false when the link should be sent again
// later (fbs unavailable or unreachable, or the key refused).
func (s *Sender) post(ctx context.Context, ref, evidence string) (result string, final bool) {
	body := map[string]string{"ref": ref, "source": "igs"}
	if evidence != "" && len(evidence) <= 2048 {
		body["evidence_url"] = evidence
	}
	b, _ := json.Marshal(body)
	ctx, cancel := context.WithTimeout(ctx, s.Cfg.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(s.Cfg.URL, "/")+Path, bytes.NewReader(b))
	if err != nil {
		return "failed", false
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+s.Cfg.Key)
	c := s.HTTP
	if c == nil {
		c = http.DefaultClient
	}
	resp, err := c.Do(req)
	if err != nil {
		return "failed", false
	}
	defer func() { _ = resp.Body.Close() }()
	switch {
	case resp.StatusCode == http.StatusAccepted:
		var out struct {
			Result string `json:"result"`
		}
		_ = json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&out)
		if Results[out.Result] {
			return out.Result, true
		}
		return "other", true
	case resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusTooManyRequests:
		return "unavailable", false
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		return "refused", false
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		// fbs won't take this link: not sent again.
		return "invalid", true
	}
	return "failed", false
}

var bioURL = regexp.MustCompile(`(?i)\b(?:https?://)?(?:[a-z0-9-]+\.)*(?:facebook\.com|fb\.com|fb\.me)/[^\s<>"'()\[\]{}]+`)

// Links are the distinct Facebook links on a profile, as found: in the bio
// text, the bio links (their JSON list), the website and, as its page
// URL, the linked Facebook page.
func Links(bio, bioLinks, website, pageID string) []string {
	var cands []string
	for _, m := range bioURL.FindAllString(bio, -1) {
		cands = append(cands, strings.TrimRight(m, ".,;:!?…،"))
	}
	var bl []struct {
		URL string `json:"url"`
	}
	if bioLinks != "" && json.Unmarshal([]byte(bioLinks), &bl) == nil {
		for _, l := range bl {
			cands = append(cands, l.URL)
		}
	}
	cands = append(cands, website)
	if pageID != "" {
		cands = append(cands, "https://www.facebook.com/"+pageID)
	}
	var out []string
	seen := map[string]bool{}
	for _, c := range cands {
		c = strings.TrimSpace(c)
		if c == "" || len(c) > 2048 || !IsFacebook(c) {
			continue
		}
		if k := Key(c); !seen[k] {
			seen[k] = true
			out = append(out, c)
		}
	}
	return out
}

func parse(raw string) *url.URL {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		u, err = url.Parse("https://" + raw)
	}
	if err != nil || u.Host == "" {
		return nil
	}
	return u
}

// IsFacebook says whether raw links to a page on Facebook: facebook.com,
// fb.com or fb.me with a path, not through Facebook's link redirect.
func IsFacebook(raw string) bool {
	u := parse(raw)
	if u == nil {
		return false
	}
	host := strings.ToLower(u.Hostname())
	d, err := publicsuffix.EffectiveTLDPlusOne(host)
	if err != nil || (d != "facebook.com" && d != "fb.com" && d != "fb.me") {
		return false
	}
	if strings.HasPrefix(host, "l.") || strings.HasPrefix(host, "lm.") {
		return false
	}
	return strings.Trim(u.Path, "/") != ""
}

// Key is the link igs deduplicates by: lower-case host without www. or m.,
// lower-case path without the trailing /, and only profile.php's id.
func Key(raw string) string {
	u := parse(raw)
	if u == nil {
		return strings.ToLower(raw)
	}
	host := strings.ToLower(u.Hostname())
	for _, p := range []string{"www.", "m.", "web.", "mobile."} {
		host = strings.TrimPrefix(host, p)
	}
	path := strings.TrimRight(strings.ToLower(u.Path), "/")
	k := host + path
	if strings.HasSuffix(path, "/profile.php") {
		if id := u.Query().Get("id"); id != "" {
			k += "?id=" + id
		}
	}
	return k
}
