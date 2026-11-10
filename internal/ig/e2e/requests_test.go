package e2e

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	contentv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/content/v1"
	registryv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/registry/v1"

	accountreq "github.com/abdullah95k/securebrand-microservices-igs/internal/ig/requests"
)

// The platform's account requests (REQ-019 item 4.3) on igs's schedule:
// add on a tier, move it, pause, resume, collect once now (a redelivery is
// the same job), and another platform's request is left alone.
func TestAccountRequests(t *testing.T) {
	s := Start(t, Options{Spec: spec()})
	ctx := context.Background()
	h := &accountreq.Handler{DB: s.DB, Dispatch: s.Dispatch, Cfg: s.Cfg}
	p := s.World.Profiles()[0]
	acct := &contentv1.AccountRef{Handle: p.Username}
	apply := func(r *registryv1.AccountRequest) {
		t.Helper()
		r.Platform, r.Account = "ig", acct
		if err := h.Apply(ctx, r); err != nil {
			t.Fatalf("%v: %v", r.Action, err)
		}
	}
	schedule := func() (tier string, enabled bool) {
		t.Helper()
		if err := s.DB.QueryRow(ctx, `SELECT tier, enabled FROM profile_schedule WHERE profile_ref = $1`, "u:"+p.Username).
			Scan(&tier, &enabled); err != nil {
			t.Fatal(err)
		}
		return tier, enabled
	}
	apply(&registryv1.AccountRequest{RequestId: "r1", Action: registryv1.Action_ACTION_ADD, Tier: registryv1.Tier_TIER_HOT, RequestedBy: "client"})
	if tier, on := schedule(); tier != "hot" || !on {
		t.Fatalf("after add: %s, enabled %v", tier, on)
	}
	apply(&registryv1.AccountRequest{RequestId: "r2", Action: registryv1.Action_ACTION_UPDATE, Tier: registryv1.Tier_TIER_WARM})
	if tier, _ := schedule(); tier != "warm" {
		t.Errorf("after update: %s", tier)
	}
	apply(&registryv1.AccountRequest{RequestId: "r3", Action: registryv1.Action_ACTION_PAUSE})
	if _, on := schedule(); on {
		t.Error("still enabled after pause")
	}
	apply(&registryv1.AccountRequest{RequestId: "r4", Action: registryv1.Action_ACTION_RESUME})
	if _, on := schedule(); !on {
		t.Error("not enabled after resume")
	}
	jobs := func() int {
		return count(t, s, `SELECT count(*) FROM jobs WHERE kind = 'on_demand' AND profile_ref = 'u:`+p.Username+`'`)
	}
	before := jobs()
	apply(&registryv1.AccountRequest{RequestId: "r5", Action: registryv1.Action_ACTION_COLLECT_NOW})
	apply(&registryv1.AccountRequest{RequestId: "r5", Action: registryv1.Action_ACTION_COLLECT_NOW})
	if n := jobs() - before; n != 1 {
		t.Errorf("collect now twice with one request ID: %d jobs, want 1", n)
	}
	// Another platform's request is skipped, an unknown account is dropped.
	other, _ := proto.Marshal(&registryv1.AccountRequest{RequestId: "r6", Platform: "tt", Account: acct, Action: registryv1.Action_ACTION_PAUSE})
	if err := h.Handle(ctx, &kgo.Record{Value: other}); err != nil {
		t.Fatal(err)
	}
	if _, on := schedule(); !on {
		t.Error("a TikTok request paused an Instagram profile")
	}
	missing, _ := proto.Marshal(&registryv1.AccountRequest{RequestId: "r7", Platform: "ig", Action: registryv1.Action_ACTION_PAUSE,
		Account: &contentv1.AccountRef{Handle: "nobody_here"}})
	if err := h.Handle(ctx, &kgo.Record{Value: missing}); err != nil {
		t.Errorf("a request for an account not on the schedule: %v, want it dropped", err)
	}
}
