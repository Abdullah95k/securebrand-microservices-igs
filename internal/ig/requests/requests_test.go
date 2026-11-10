package requests

import (
	"testing"

	contentv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/content/v1"
	registryv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/registry/v1"
)

// A requested account becomes igs's profile ref by its ID, else its
// handle, else its URL.
func TestRef(t *testing.T) {
	for _, c := range []struct {
		a    *contentv1.AccountRef
		want string
	}{
		{&contentv1.AccountRef{Id: "ig:account:42", Handle: "brand"}, "id:42"},
		{&contentv1.AccountRef{NativeId: "42"}, "id:42"},
		{&contentv1.AccountRef{Handle: "@Brand"}, "u:brand"},
		{&contentv1.AccountRef{Url: "https://www.instagram.com/brand/"}, "u:brand"},
	} {
		if got, err := Ref(c.a); err != nil || got != c.want {
			t.Errorf("Ref(%v) = %q, %v; want %q", c.a, got, err, c.want)
		}
	}
	for _, a := range []*contentv1.AccountRef{nil, {}, {Handle: "not a handle!"}} {
		if got, err := Ref(a); err == nil {
			t.Errorf("Ref(%v) = %q, want an error", a, got)
		}
	}
}

func TestTier(t *testing.T) {
	for in, want := range map[registryv1.Tier]string{registryv1.Tier_TIER_HOT: "hot", registryv1.Tier_TIER_WARM: "warm",
		registryv1.Tier_TIER_LONG_TAIL: "long_tail", registryv1.Tier_TIER_DORMANT: "long_tail", registryv1.Tier_TIER_UNSPECIFIED: "auto"} {
		if got := Tier(in); got != want {
			t.Errorf("Tier(%v) = %q, want %q", in, got, want)
		}
	}
}
