package log

import (
	"strings"
	"testing"
)

// An account is named by its pk, or by a stable hash of its reference:
// never by the username the reference holds.
func TestAccountName(t *testing.T) {
	if got := AccountName(1000000001, "u:fake_user_1"); got != "pk:1000000001" {
		t.Errorf("with a pk: %q", got)
	}
	if got := AccountName(0, "id:1000000002"); got != "pk:1000000002" {
		t.Errorf("an id reference: %q", got)
	}
	a, b := AccountName(0, "u:fake_user_1"), AccountName(0, "u:FAKE_USER_1")
	if a != b || !strings.HasPrefix(a, "ref#") || len(a) != len("ref#")+10 || strings.Contains(a, "fake") {
		t.Errorf("a username reference: %q %q", a, b)
	}
	if AccountName(0, "u:fake_user_2") == a {
		t.Error("two references, one name")
	}
	if got := AccountName(0, ""); got != "" {
		t.Errorf("nothing: %q", got)
	}
}
