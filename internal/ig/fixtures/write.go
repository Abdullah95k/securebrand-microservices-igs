package fixtures

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Case is one parser fixture to write: a raw response and what its parser
// needs beside it.
type Case struct {
	// Query is the parser's name (profile, grid, comments, reels,
	// highlights, post_page, reels_page) and Name the case's.
	Query, Name string
	// Raw is the response as captured; HTML pages keep only the payloads
	// holding one of Wanted.
	Raw    []byte
	HTML   bool
	Wanted []string
	// Meta is written to meta.json; Shortcode (for post pages) is mapped to
	// its pseudonym.
	Meta map[string]any
}

// Write scrubs c with s and writes testdata/ig/<query>/<name>/ under root:
// response.ndjson or response.html, and meta.json. It writes nothing and
// returns an error when a personal value of the original would survive.
func (s *Scrubber) Write(root string, c Case) (string, error) {
	var out []byte
	var err error
	if c.HTML {
		out, err = s.HTML(c.Raw, c.Wanted...)
	} else {
		out, err = s.JSON(c.Raw)
	}
	if err != nil {
		return "", err
	}
	if leaks := s.Leaks(out); len(leaks) > 0 {
		show := leaks
		if len(show) > 3 {
			show = show[:3]
		}
		for i, l := range show {
			if len(l) > 12 {
				show[i] = l[:4] + "…"
			}
		}
		return "", fmt.Errorf("fixtures: %s/%s: %d personal values survived (e.g. %q)", c.Query, c.Name, len(leaks), show)
	}
	dir := filepath.Join(root, c.Query, c.Name)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", err
	}
	name := "response.ndjson"
	if c.HTML {
		name = "response.html"
	}
	if err := os.WriteFile(filepath.Join(dir, name), out, 0o600); err != nil {
		return "", err
	}
	meta := map[string]any{"__scrubbed": true, "__scrubber": Version, "source": "probe"}
	for k, v := range c.Meta {
		meta[k] = v
	}
	if code, ok := meta["shortcode"].(string); ok {
		if p, ok := s.codes[code]; ok {
			meta["shortcode"] = p
		} else {
			return "", fmt.Errorf("fixtures: %s/%s: shortcode not in the response", c.Query, c.Name)
		}
	}
	for k, v := range meta {
		if str, ok := v.(string); ok && s.originals[str] {
			return "", fmt.Errorf("fixtures: %s/%s: meta %s holds a personal value", c.Query, c.Name, k)
		}
	}
	b, _ := json.MarshalIndent(meta, "", "  ")
	if err := os.WriteFile(filepath.Join(dir, "meta.json"), append(b, '\n'), 0o600); err != nil {
		return "", err
	}
	return dir, nil
}

// QueryOf names the parser of a probe raw file (NN-<query>-<class>.json, or
// post-page.html / page.html).
func QueryOf(file string) string {
	base := filepath.Base(file)
	switch base {
	case "post-page.html":
		return "post_page"
	case "page.html":
		return "profile_page"
	}
	parts := strings.SplitN(strings.TrimSuffix(base, filepath.Ext(base)), "-", 3)
	if len(parts) < 3 {
		return ""
	}
	q := parts[1]
	switch q {
	case "grid_first", "grid_other_identity", "grid_by_pk":
		q = "grid"
	}
	return q
}
