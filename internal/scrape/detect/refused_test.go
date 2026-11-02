package detect

import "testing"

// HTTP 400, 401 and 403 used to classify as OK when no error code or wall
// path explained them. Their bodies decide; the fallback is a soft error.
func TestRefusedStatuses(t *testing.T) {
	r := DefaultRules()
	r.BlockMarkers = append(r.BlockMarkers, []byte(`Please wait a few minutes`))
	r.LoginMarkers = append(r.LoginMarkers, []byte(`"require_login":true`))
	for _, tc := range []struct {
		name   string
		in     Input
		want   Class
		expect string
	}{
		{"401 wait", Input{Status: 401, Body: []byte(`{"message":"Please wait a few minutes before you try again.","require_login":true}`), Expect: "graphql"}, RateLimited, ""},
		{"401 login", Input{Status: 401, Body: []byte(`{"require_login":true,"status":"fail"}`), Expect: "graphql"}, LoginWall, ""},
		{"403 checkpoint", Input{Status: 403, Body: []byte(`{"message":"checkpoint_required"}`), Expect: "graphql"}, Checkpoint, ""},
		{"403 bare", Input{Status: 403, Body: []byte(`forbidden`), Expect: "html"}, SoftError, ""},
		{"400 bare", Input{Status: 400, Body: []byte(`{}`), Expect: "graphql"}, SoftError, ""},
		// A known error code still wins over the status.
		{"400 stale", Input{Status: 400, Errors: []GQLError{{Code: 1675012, Message: "PersistedQueryNotFound"}}, Expect: "graphql"}, RegistryStale, ""},
		{"200 ok", Input{Status: 200, Body: []byte(`{"data":{}}`), HasData: true, Expect: "graphql"}, OK, ""},
	} {
		if got, reason := r.Classify(tc.in); got != tc.want {
			t.Errorf("%s: %v (%s), want %v", tc.name, got, reason, tc.want)
		}
	}
}

// Configuration extends the built-in rules, or replaces the lists it sets.
func TestApplyConfig(t *testing.T) {
	base := DefaultRules()
	r, err := base.Apply(Config{RateLimitCodes: []int64{999}, StaleMessages: []string{`(?i)query hash`}, LoginPaths: []string{"/accounts/login"}})
	if err != nil {
		t.Fatal(err)
	}
	if c, _ := r.Classify(Input{Status: 200, Errors: []GQLError{{Code: 999}}, Expect: "graphql"}); c != RateLimited {
		t.Fatalf("added code: %v", c)
	}
	if c, _ := r.Classify(Input{Status: 200, Errors: []GQLError{{Code: 1675004}}, Expect: "graphql"}); c != RateLimited {
		t.Fatalf("built-in code lost: %v", c)
	}
	if c, _ := r.Classify(Input{Status: 200, Errors: []GQLError{{Message: "Unknown Query Hash"}}, Expect: "graphql"}); c != RegistryStale {
		t.Fatalf("added stale message: %v", c)
	}
	if len(base.RateLimitCodes) != 4 || len(base.LoginPaths) != 1 {
		t.Fatal("Apply must not change the rules it was called on")
	}
	rep, err := base.Apply(Config{Replace: true, LoginPaths: []string{"/accounts/login"}})
	if err != nil {
		t.Fatal(err)
	}
	if c, _ := rep.Classify(Input{Status: 302, Location: "https://x/login/?next=/", Expect: "html"}); c == LoginWall {
		t.Fatal("replace kept the built-in login path")
	}
	if len(rep.RateLimitCodes) != 4 {
		t.Fatal("replace changed a list the config did not set")
	}
	if _, err := base.Apply(Config{StaleMessages: []string{"("}}); err == nil {
		t.Fatal("an invalid regular expression must be an error")
	}
}
