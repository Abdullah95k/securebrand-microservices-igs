// Package detect classifies Facebook responses so the session layer knows
// whether to continue, back off, rotate the identity, or give up on content.
package detect

import (
	"bytes"
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// Class is the outcome category of one request.
type Class int

const (
	OK Class = iota
	// RateLimited: back off this identity and retry elsewhere.
	RateLimited
	// LoginWall: logged-out access stopped here. Repeated at the same cursor
	// on fresh identities it means coverage is truncated.
	LoginWall
	// Checkpoint: identity/IP flagged. Retire it.
	Checkpoint
	// SoftError: transient server error. Re-bootstrap tokens and retry once.
	SoftError
	// RegistryStale: doc_id or variables no longer accepted.
	RegistryStale
	// SchemaDrift: response shape changed; parser could not find required fields.
	SchemaDrift
	// ContentUnavailable: the node is deleted, restricted or geo-blocked.
	ContentUnavailable
	// Transport: network/proxy failure.
	Transport
)

var classNames = [...]string{"ok", "rate_limited", "login_wall", "checkpoint", "soft_error",
	"registry_stale", "schema_drift", "content_unavailable", "transport"}

func (c Class) String() string {
	if int(c) < len(classNames) {
		return classNames[c]
	}
	return "unknown"
}

// Blocking reports whether the class counts against the identity's health.
func (c Class) Blocking() bool {
	return c == RateLimited || c == LoginWall || c == Checkpoint
}

// GQLError is one entry of a GraphQL errors array or the legacy top-level
// {"error": code, "errorSummary": ...} form.
type GQLError struct {
	Code     int64
	Message  string
	Severity string
	Summary  string
}

// Rules holds a platform's error codes, wall paths and markers. Facebook's
// come from DefaultRules; another platform passes its own, so nothing here is
// specific to one of them. Defaults come from public observations and must
// be confirmed in Phase 0.
type Rules struct {
	RateLimitCodes []int64
	SoftCodes      []int64
	LoginCodes     []int64
	StaleMessages  []*regexp.Regexp
	// LoginMarkers mark an HTML login page returned where GraphQL JSON was
	// expected; BlockMarkers a block page, on either kind of request.
	LoginMarkers [][]byte
	BlockMarkers [][]byte
	// CheckpointPaths and LoginPaths classify a redirect whose Location
	// contains one of them, checkpoints first.
	CheckpointPaths []string
	LoginPaths      []string
	// CheckpointPageMarkers and LoginPageMarkers classify a page load (Expect
	// "html") that came back as a challenge or a login wall. Facebook needs
	// no login page markers: its logged-out pages carry a login form anyway.
	CheckpointPageMarkers [][]byte
	LoginPageMarkers      [][]byte
	// CheckpointMarkers mark a challenge in the body of an HTTP 400, 401 or
	// 403 (Instagram's {"message":"checkpoint_required"}). On those statuses
	// BlockMarkers mean a rate limit, LoginMarkers a login wall, and anything
	// else a soft error: never OK.
	CheckpointMarkers [][]byte
}

// Config extends (or, with Replace, replaces) a platform's built-in rules
// from configuration, so a newly observed error code or marker needs no
// release. Empty lists change nothing.
type Config struct {
	Replace               bool     `koanf:"replace"`
	RateLimitCodes        []int64  `koanf:"rate_limit_codes"`
	SoftCodes             []int64  `koanf:"soft_codes"`
	LoginCodes            []int64  `koanf:"login_codes"`
	StaleMessages         []string `koanf:"stale_messages"` // regular expressions
	LoginMarkers          []string `koanf:"login_markers"`
	BlockMarkers          []string `koanf:"block_markers"`
	CheckpointMarkers     []string `koanf:"checkpoint_markers"`
	CheckpointPaths       []string `koanf:"checkpoint_paths"`
	LoginPaths            []string `koanf:"login_paths"`
	CheckpointPageMarkers []string `koanf:"checkpoint_page_markers"`
	LoginPageMarkers      []string `koanf:"login_page_markers"`
}

// Apply returns r with c's lists appended, or, with c.Replace, each list
// c sets in place of r's. A stale message that is not a valid regular
// expression is an error.
func (r Rules) Apply(c Config) (Rules, error) {
	codes := func(dst *[]int64, src []int64) {
		if len(src) == 0 {
			return
		}
		if c.Replace {
			*dst = nil
		}
		*dst = append(slices.Clone(*dst), src...)
	}
	markers := func(dst *[][]byte, src []string) {
		if len(src) == 0 {
			return
		}
		if c.Replace {
			*dst = nil
		}
		out := slices.Clone(*dst)
		for _, m := range src {
			out = append(out, []byte(m))
		}
		*dst = out
	}
	paths := func(dst *[]string, src []string) {
		if len(src) == 0 {
			return
		}
		if c.Replace {
			*dst = nil
		}
		*dst = append(slices.Clone(*dst), src...)
	}
	codes(&r.RateLimitCodes, c.RateLimitCodes)
	codes(&r.SoftCodes, c.SoftCodes)
	codes(&r.LoginCodes, c.LoginCodes)
	if len(c.StaleMessages) > 0 {
		if c.Replace {
			r.StaleMessages = nil
		}
		stale := slices.Clone(r.StaleMessages)
		for _, m := range c.StaleMessages {
			re, err := regexp.Compile(m)
			if err != nil {
				return r, fmt.Errorf("detect: stale message %q: %w", m, err)
			}
			stale = append(stale, re)
		}
		r.StaleMessages = stale
	}
	markers(&r.LoginMarkers, c.LoginMarkers)
	markers(&r.BlockMarkers, c.BlockMarkers)
	markers(&r.CheckpointMarkers, c.CheckpointMarkers)
	paths(&r.CheckpointPaths, c.CheckpointPaths)
	paths(&r.LoginPaths, c.LoginPaths)
	markers(&r.CheckpointPageMarkers, c.CheckpointPageMarkers)
	markers(&r.LoginPageMarkers, c.LoginPageMarkers)
	return r, nil
}

// DefaultRules returns the built-in rules.
func DefaultRules() Rules {
	return Rules{
		RateLimitCodes: []int64{1675004, 1675007, 368, 1390008},
		SoftCodes:      []int64{1357004, 1357001, 1357031, 2},
		LoginCodes:     []int64{1357005, 1348007},
		StaleMessages: []*regexp.Regexp{
			regexp.MustCompile(`(?i)persisted ?query`),
			regexp.MustCompile(`(?i)\bdoc_?id\b`),
			regexp.MustCompile(`(?i)unknown (query|operation)`),
			regexp.MustCompile(`(?i)variable .* (is|was) (not|never) (defined|provided)`),
		},
		LoginMarkers:          [][]byte{[]byte(`id="login_form"`), []byte(`"loginForm"`), []byte(`/login/?next=`), []byte(`login_required`)},
		BlockMarkers:          []([]byte){[]byte(`temporarily blocked`), []byte(`Temporarily Blocked`), []byte(`rate limit`)},
		CheckpointPaths:       []string{"/checkpoint"},
		LoginPaths:            []string{"/login"},
		CheckpointPageMarkers: [][]byte{[]byte(`action="/checkpoint`)},
		CheckpointMarkers:     [][]byte{[]byte(`checkpoint_required`)},
	}
}

// Input is everything the classifier looks at.
type Input struct {
	Status   int
	Location string
	// ContentType of the response, e.g. "text/html; charset=utf-8".
	ContentType string
	Body        []byte
	Errors      []GQLError
	// HasData reports that a GraphQL response carries data. Errors beside
	// it that are not critical are partial (Facebook streams pages with
	// "field_exception" errors for single fields) and leave it OK.
	HasData bool
	// Expect is "graphql" for API calls and "html" for page loads.
	Expect string
}

// Classify returns the class and a short reason.
func (r Rules) Classify(in Input) (Class, string) {
	if loc := strings.ToLower(in.Location); loc != "" {
		for _, p := range r.CheckpointPaths {
			if strings.Contains(loc, p) {
				return Checkpoint, "redirect to checkpoint"
			}
		}
		for _, p := range r.LoginPaths {
			if strings.Contains(loc, p) {
				return LoginWall, "redirect to login"
			}
		}
	}
	switch {
	case in.Status == 429:
		return RateLimited, "http 429"
	case in.Status == 407 || in.Status == 502 || in.Status == 503 || in.Status == 504:
		return Transport, "http " + itoa(in.Status)
	case in.Status >= 500:
		return SoftError, "http " + itoa(in.Status)
	case in.Status == 404 && in.Expect == "html":
		return ContentUnavailable, "http 404"
	case in.Status >= 300 && in.Status < 400:
		// Unknown redirect (consent, locale); treat as soft so the caller
		// re-bootstraps once.
		return SoftError, "redirect " + in.Location
	}
	for _, e := range in.Errors {
		switch {
		case contains(r.RateLimitCodes, e.Code):
			return RateLimited, "fb error " + i64toa(e.Code)
		case contains(r.LoginCodes, e.Code):
			return LoginWall, "fb error " + i64toa(e.Code)
		}
		for _, re := range r.StaleMessages {
			if re.MatchString(e.Message) || re.MatchString(e.Summary) {
				return RegistryStale, "stale: " + truncate(e.Message, 80)
			}
		}
		if contains(r.SoftCodes, e.Code) {
			return SoftError, "fb error " + i64toa(e.Code)
		}
	}
	if in.Expect == "graphql" && isHTML(in) {
		head := in.Body
		if len(head) > 256<<10 {
			head = head[:256<<10]
		}
		for _, m := range r.BlockMarkers {
			if bytes.Contains(head, m) {
				return RateLimited, "block page"
			}
		}
		for _, m := range r.LoginMarkers {
			if bytes.Contains(head, m) {
				return LoginWall, "login page"
			}
		}
		return SoftError, "html instead of graphql"
	}
	if in.Expect == "html" {
		head := in.Body
		if len(head) > 512<<10 {
			head = head[:512<<10]
		}
		for _, m := range r.CheckpointPageMarkers {
			if bytes.Contains(head, m) {
				return Checkpoint, "checkpoint page"
			}
		}
		for _, m := range r.LoginPageMarkers {
			if bytes.Contains(head, m) {
				return LoginWall, "login page"
			}
		}
		for _, m := range r.BlockMarkers {
			if bytes.Contains(head, m) {
				return RateLimited, "block page"
			}
		}
	}
	if in.Status == 400 || in.Status == 401 || in.Status == 403 {
		return r.refused(in)
	}
	if len(in.Errors) > 0 && (!in.HasData || critical(in.Errors)) {
		return SoftError, "fb error " + i64toa(in.Errors[0].Code) + " " + truncate(in.Errors[0].Message, 60)
	}
	return OK, ""
}

// refused classifies an HTTP 400, 401 or 403 that no error code or wall
// path explained, by its body: a block marker is a rate limit (Instagram
// answers "Please wait a few minutes" with 401 or 403), a checkpoint marker
// a checkpoint, a login marker a login wall. Anything else is a soft error:
// these used to classify as OK.
func (r Rules) refused(in Input) (Class, string) {
	head := in.Body
	if len(head) > 64<<10 {
		head = head[:64<<10]
	}
	reason := "http " + itoa(in.Status)
	for _, m := range r.BlockMarkers {
		if bytes.Contains(head, m) {
			return RateLimited, reason
		}
	}
	for _, m := range r.CheckpointMarkers {
		if bytes.Contains(head, m) {
			return Checkpoint, reason
		}
	}
	for _, m := range r.LoginMarkers {
		if bytes.Contains(head, m) {
			return LoginWall, reason
		}
	}
	return SoftError, reason
}

// isHTML reports whether a GraphQL response is an HTML page. Facebook labels
// its GraphQL JSON text/html too, so the content type alone says nothing
// when the body is JSON (or JSON behind the for (;;); guard).
func isHTML(in Input) bool {
	body := bytes.TrimSpace(in.Body)
	if bytes.HasPrefix(body, []byte("<")) {
		return true
	}
	jsonBody := bytes.HasPrefix(body, []byte("{")) || bytes.HasPrefix(body, []byte("[")) || bytes.HasPrefix(body, []byte("for (;;);"))
	return !jsonBody && strings.Contains(in.ContentType, "text/html")
}

func critical(errs []GQLError) bool {
	for _, e := range errs {
		if strings.EqualFold(e.Severity, "CRITICAL") {
			return true
		}
	}
	return false
}

func contains(xs []int64, x int64) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

func itoa(i int) string { return i64toa(int64(i)) }

func i64toa(i int64) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		p--
		b[p] = '-'
	}
	return string(b[p:])
}
