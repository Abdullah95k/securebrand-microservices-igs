package probe

import "testing"

// An exit rests on a wall before its first good page load, or on two walls
// in a row; a wall between good loads (a profile walled for everyone, not
// the exit) does not rest it.
func TestPageLoadRestRule(t *testing.T) {
	var sh shared
	if !sh.pageLoad("a", true) {
		t.Fatal("a wall with no good load yet rests the exit")
	}
	sh.pageLoad("b", false)
	if sh.pageLoad("b", true) {
		t.Fatal("one wall after a good load must not rest it")
	}
	sh.pageLoad("b", false)
	if sh.pageLoad("b", true) {
		t.Fatal("a good load resets the streak")
	}
	if !sh.pageLoad("b", true) {
		t.Fatal("two walls in a row rest it")
	}
}
