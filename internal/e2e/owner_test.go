package e2e

import (
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/api"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakefb"
)

// A job is its client's: another client's GET, cancel and results answer
// 404 (as if it did not exist), the owner's work, and an admin sees all.
// Only the list filtered by owner before (brief §5 item 3).
func TestJobsAreVisibleToTheirOwnerOnly(t *testing.T) {
	s := Start(t, Options{Spec: fakefb.Spec{Seed: 21, Pages: 1, PostsPerPage: 2, PostSpacing: 20 * time.Hour, MaxComments: 2}})
	owner := s.Key(t, api.ScopeJobsWrite, api.ScopeDataRead)
	other := s.Key(t, api.ScopeJobsWrite, api.ScopeDataRead)
	admin := s.Key(t, api.ScopeAdmin)
	page := s.Fake.World.Pages()[0]
	j := s.runJobWithin(t, owner, page.Vanity, 2*time.Minute)

	status := func(key, method, path string) int {
		t.Helper()
		resp := s.Do(t, key, method, path, nil)
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp.StatusCode
	}
	for _, tc := range []struct {
		name, key, method, path string
		want                    int
	}{
		{"owner get", owner, "GET", "/v1/jobs/" + j.ID, http.StatusOK},
		{"owner results", owner, "GET", "/v1/jobs/" + j.ID + "/results", http.StatusOK},
		{"other get", other, "GET", "/v1/jobs/" + j.ID, http.StatusNotFound},
		{"other results", other, "GET", "/v1/jobs/" + j.ID + "/results", http.StatusNotFound},
		{"other cancel", other, "POST", "/v1/jobs/" + j.ID + "/cancel", http.StatusNotFound},
		{"admin get", admin, "GET", "/v1/jobs/" + j.ID, http.StatusOK},
	} {
		if got := status(tc.key, tc.method, tc.path); got != tc.want {
			t.Errorf("%s: %s %s = %d, want %d", tc.name, tc.method, tc.path, got, tc.want)
		}
	}
	// The other client's list does not show it either.
	type jobList struct {
		Jobs []jobView `json:"jobs"`
	}
	for _, x := range decodeBody[jobList](t, s.Do(t, other, "GET", "/v1/jobs", nil), http.StatusOK).Jobs {
		if x.ID == j.ID {
			t.Fatalf("job %s listed for another client", j.ID)
		}
	}
}
