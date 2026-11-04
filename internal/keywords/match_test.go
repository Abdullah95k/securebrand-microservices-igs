package keywords

import (
	"slices"
	"testing"
)

func seedMatcher(t *testing.T) *Matcher {
	t.Helper()
	seed, err := Seed()
	if err != nil {
		t.Fatal(err)
	}
	return SeedMatcher(seed)
}

func groups(m *Matcher, text string) []string {
	var out []string
	for g := range m.Groups(text) {
		out = append(out, g)
	}
	slices.Sort(out)
	return out
}

// The shared list names places in every script and form a bio uses.
func TestMatcherFindsPlaces(t *testing.T) {
	m := seedMatcher(t)
	cases := []struct {
		text string
		want []string
	}{
		{"Makeup artist | Baghdad 🇮🇶", []string{"baghdad"}},
		{"Live from HEWLÊR tonight", []string{"erbil"}},
		{"مصورة في بغداد", []string{"baghdad"}},
		{"وبغداد والبصرة", []string{"baghdad", "basra"}}, // attached و
		{"بالبصرة", []string{"basra"}},                   // ب + article
		{"فخور بأني بغدادي", []string{"baghdad"}},        // nisba
		{"صفحة العراقيين في الخارج", []string{"iraq"}},   // العراق + يين
		{"ئاهەنگ لە هه\u200cولێر", []string{"erbil"}},    // Sorani, ZWNJ
		{"Kurdistan Region of Iraq", []string{"iraq", "kurdistan_region"}},
		{"Iraqis in Europe", []string{"iraq"}},      // added from igs
		{"Dhi Qar", []string{"dhi_qar"}},            // two words
		{"#erbil #iraq", []string{"erbil", "iraq"}}, // hashtags
		{"مدينة الصدر", []string{"baghdad"}},
	}
	for _, c := range cases {
		if got := groups(m, c.text); !slices.Equal(got, c.want) {
			t.Errorf("%q: %v, want %v", c.text, got, c.want)
		}
	}
}

// A place name inside a longer word, and the seed's "off" spellings, name
// nothing.
func TestMatcherIgnoresWordsThatHoldAName(t *testing.T) {
	m := seedMatcher(t)
	for _, text := range []string{
		"عراقة وأصالة",       // "heritage and authenticity" holds عراق
		"رحلة سعيدة",         // "a happy trip" holds حلة (and حلة is off)
		"Kalara studio",      // holds Kalar
		"Hit the gym",        // Hit is off
		"Amara and friends",  // Amara is off
		"Kut",                // off; Al-Kut is on
		"قائد الفريق القائم", // القائم is off
		"Suleimani family",   // not a listed spelling
		"Dubai, UAE",
	} {
		if got := groups(m, text); len(got) > 0 {
			t.Errorf("%q names %v", text, got)
		}
	}
	if got := groups(m, "Al-Kut"); !slices.Equal(got, []string{"wasit"}) {
		t.Errorf("Al-Kut: %v", got)
	}
}

// Every enabled seed term finds itself, so no spelling is unreachable.
func TestEverySeedTermMatchesItself(t *testing.T) {
	seed, err := Seed()
	if err != nil {
		t.Fatal(err)
	}
	m := SeedMatcher(seed)
	for _, k := range seed {
		if !k.Enabled {
			continue
		}
		if !m.Groups(k.Term)[k.Group] {
			t.Errorf("%s %q (%s) does not match itself", k.Language, k.Term, k.Group)
		}
	}
}

// igs's additions keep fbs's file as is: fbs's groups, no spelling twice.
func TestSeedAddsIGSSpellingsToFBS(t *testing.T) {
	fbs, err := IraqSeed()
	if err != nil {
		t.Fatal(err)
	}
	all, err := Seed()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) < len(fbs) || !slices.Equal(all[:len(fbs)], fbs) {
		t.Fatalf("Seed() does not start with fbs's %d keywords unchanged", len(fbs))
	}
	groups := map[string]bool{}
	for _, k := range fbs {
		groups[k.Group] = true
	}
	seen := map[[2]string]bool{}
	for _, k := range all {
		if !groups[k.Group] {
			t.Errorf("%q: group %q is not one of fbs's", k.Term, k.Group)
		}
		key := [2]string{Normalize(k.Term), k.Language}
		if seen[key] {
			t.Errorf("%s %q listed twice", k.Language, k.Term)
		}
		seen[key] = true
	}
}
