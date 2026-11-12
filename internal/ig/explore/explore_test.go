package explore

import "testing"

func TestPagePath(t *testing.T) {
	for _, c := range []struct{ kind, key, want string }{
		{KindHashtag, "#Baghdad", "/explore/tags/baghdad/"},
		{KindHashtag, "بغداد", "/explore/tags/%D8%A8%D8%BA%D8%AF%D8%A7%D8%AF/"},
		{KindLocation, "1000001", "/explore/locations/1000001/"},
		{KindHashtag, "#", ""},
		{KindLocation, " ", ""},
		{"profile", "x", ""},
	} {
		if got := PagePath(c.kind, c.key); got != c.want {
			t.Errorf("PagePath(%q, %q) = %q, want %q", c.kind, c.key, got, c.want)
		}
	}
}

// Owners are the distinct accounts that posted a page's posts, by
// username, with their pk when shown; an owner with no username is left
// out, and objects that are not posts are ignored.
func TestOwners(t *testing.T) {
	html := []byte(`<html><script type="application/json">{"top":{"edges":[
		{"node":{"pk":"1","code":"A","user":{"username":"Lens_One","id":"11"}}},
		{"node":{"pk":"2","code":"B","owner":{"pk":"11"},"user":{"username":"lens_one"}}},
		{"node":{"pk":"3","code":"C","owner":{"id":"22"}}},
		{"node":{"id":"4","shortcode":"D","user":{"username":"cafe_two","pk":"33"}}},
		{"user":{"username":"not_a_post"}}]}}</script></html>`)
	got := Owners(html)
	if len(got) != 2 || got[0] != (Owner{PK: 11, Username: "Lens_One"}) || got[1] != (Owner{PK: 33, Username: "cafe_two"}) {
		t.Fatalf("Owners = %+v", got)
	}
	if Owners([]byte("<html>no data</html>")) != nil && len(Owners([]byte("<html></html>"))) != 0 {
		t.Fatal("owners on a page with no data")
	}
}
