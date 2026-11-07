package discovery

import (
	"testing"
)

func f(v float64) *float64 { return &v }

// Each signal on its own, as Score weighs it.
func TestScoreSignals(t *testing.T) {
	for _, tc := range []struct {
		name string
		vet  Vet
		s    Sightings
		want Signals
	}{
		{"iraq named in English", Vet{Biography: "Proud Iraqi photographer"}, Sightings{}, Signals{"iraq_named": 3}},
		{"iraq named in Arabic with an article", Vet{Biography: "مصورة من العراق"}, Sightings{},
			Signals{"iraq_named": 3, "arabic_script": 1}},
		{"flag", Vet{FullName: "Ali 🇮🇶"}, Sightings{}, Signals{"iraq_named": 3}},
		{"phone, international", Vet{Biography: "orders: +964 770 123 4567"}, Sightings{}, Signals{"iraq_phone": 3}},
		{"phone, local, Arabic-Indic digits", Vet{Biography: "للحجز ٠٧٨٠١٢٣٤٥٦٧ واتساب فقط والتوصيل"}, Sightings{},
			Signals{"iraq_phone": 3, "arabic_script": 1}},
		{"iq link", Vet{Links: []string{"https://shop.example.iq/x"}}, Sightings{}, Signals{"iq_link": 2}},
		{"one city, with a preposition", Vet{Biography: "مطعم ببغداد شارع فلسطين"}, Sightings{},
			Signals{"iraq_city": 2, "arabic_script": 1, "other_country": 0}},
		{"two cities", Vet{Biography: "Branches in Basra and Erbil"}, Sightings{}, Signals{"iraq_city": 3}},
		{"city in captions only", Vet{Captions: []string{"weekend in Najaf"}}, Sightings{}, Signals{"iraq_city": 1}},
		// Places count by governorate (the shared list's groups): Dhi Qar
		// and its capital Nasiriyah are one, Dhi Qar and Erbil two.
		{"two-word province and its capital", Vet{Biography: "من ذي قار والناصرية"}, Sightings{}, Signals{"iraq_city": 2, "arabic_script": 1}},
		{"two-word province and another", Vet{Biography: "من ذي قار وأربيل"}, Sightings{}, Signals{"iraq_city": 3, "arabic_script": 1}},
		{"one city in two scripts", Vet{Biography: "Baghdad بغداد"}, Sightings{}, Signals{"iraq_city": 2, "arabic_script": 1}},
		{"two-word province", Vet{Biography: "من محافظة ذي قار"}, Sightings{}, Signals{"iraq_city": 2, "arabic_script": 1}},
		{"Sorani Kurdish and Erbil", Vet{Biography: "وێنەگر لە هەولێر"}, Sightings{},
			Signals{"iraq_city": 2, "sorani_kurdish": 1.5, "arabic_script": 1}},
		{"Kurdistan", Vet{Biography: "Kurdistan region"}, Sightings{}, Signals{"kurdistan": 1.5}},
		{"dialect", Vet{Captions: []string{"شلونكم يا جماعة هسه وصلت", "ماكو اجمل من هيج"}}, Sightings{},
			Signals{"iraqi_dialect": 2, "arabic_script": 1}},
		{"Arabic, another country", Vet{Biography: "مصممة ازياء في دبي الامارات"}, Sightings{},
			Signals{"arabic_script": 1, "other_country": -2}},
		{"another country but serving Iraq", Vet{Biography: "Dubai based, shipping to Iraq"}, Sightings{}, Signals{"iraq_named": 3}},
		{"nothing", Vet{Biography: "Coffee lover in London"}, Sightings{}, Signals{}},
		{"word inside a word is not a city", Vet{Biography: "رحلة جميلة الى البحر مع الاصدقاء دائما"}, Sightings{},
			Signals{"arabic_script": 1}},
		{"one registered account, many kinds of link", Vet{}, Sightings{Coauthors: 1, Tags: 2, Mentions: 1, Comments: 5, Sources: 1},
			Signals{"iraqi_audience": 1, "coauthor": 0.5}},
		{"a tag, before sources were counted", Vet{}, Sightings{Tags: 1}, Signals{"iraqi_audience": 1}},
		{"three registered accounts", Vet{}, Sightings{Comments: 3, Sources: 3}, Signals{"iraqi_audience": 2}},
		{"ten registered accounts, posts located in Iraq", Vet{}, Sightings{Tags: 10, IraqContext: 2, Sources: 10},
			Signals{"iraqi_audience": 3, "seen_in_iraq": 1.5, "network_capped": -0.5}},
		{"network evidence is capped at 4", Vet{}, Sightings{Coauthors: 3, Tags: 10, IraqContext: 2, Sources: 12},
			Signals{"iraqi_audience": 3, "seen_in_iraq": 1.5, "coauthor": 0.5, "network_capped": -1}},
		{"a world star Iraqi fans tag", Vet{Biography: "Athlete", Followers: 50_000_000}, Sightings{Tags: 40, IraqContext: 5, Sources: 30},
			Signals{"iraqi_audience": 3, "seen_in_iraq": 1.5, "network_capped": -2.5}},
		// Instagram keyword discovery (igdisc): met near the shared list's
		// places in others' recent posts and comments. 0.5 a place, at most
		// 1, inside the network cap: never enough alone.
		{"met near one place", Vet{}, Sightings{Comments: 1, Sources: 1, Keywords: 1},
			Signals{"iraqi_audience": 1, "keyword_context": 0.5}},
		{"met near two places", Vet{}, Sightings{Comments: 1, Sources: 1, Keywords: 2},
			Signals{"iraqi_audience": 1, "keyword_context": 1}},
		{"met near many places, capped at 1", Vet{}, Sightings{Comments: 1, Sources: 1, Keywords: 7},
			Signals{"iraqi_audience": 1, "keyword_context": 1}},
		{"keyword context inside the network cap", Vet{}, Sightings{Tags: 10, IraqContext: 2, Sources: 10, Keywords: 3},
			Signals{"iraqi_audience": 3, "seen_in_iraq": 1.5, "keyword_context": 1, "network_capped": -1.5}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := Score(tc.vet, tc.s)
			for k, v := range tc.want {
				if v == 0 {
					if _, ok := got[k]; ok {
						t.Errorf("%s: unexpected %s", tc.name, k)
					}
					delete(tc.want, k)
				}
			}
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for k, v := range tc.want {
				if got[k] != v {
					t.Fatalf("got %v, want %v", got, tc.want)
				}
			}
		})
	}
}

// At the default threshold (4): an Iraqi photographer, shop and Kurdish
// account pass; an Arabic account elsewhere, an English one, and an account
// known only by one tag do not.
func TestScoreAtTheDefaultThreshold(t *testing.T) {
	met := Sightings{Tags: 1, Sources: 1}
	for _, tc := range []struct {
		name string
		vet  Vet
		pass bool
	}{
		{"photographer in Baghdad", Vet{FullName: "علي", Biography: "مصور فوتوغرافي | بغداد"}, true},
		{"shop with a phone", Vet{Biography: "ملابس اطفال توصيل لكل المحافظات 07701234567"}, true},
		{"Kurdish in Sulaymaniyah", Vet{Biography: "خۆشەویستی و ژیان لە سلێمانی"}, true},
		{"dialect in captions", Vet{Captions: []string{"شلونكم حبايب", "هسه نزلت الفيديو", "ماكو مثلكم", "كلش حلو"}}, true},
		{"Arabic in Riyadh", Vet{Biography: "مصورة في الرياض السعودية"}, false},
		{"English", Vet{Biography: "Travel | Food | London"}, false},
		{"nothing but how it was met", Vet{}, false},
	} {
		sc := Score(tc.vet, met).Sum()
		if pass := sc >= 4; pass != tc.pass {
			t.Errorf("%s: score %.1f (%v), want pass=%v", tc.name, sc, Score(tc.vet, met), tc.pass)
		}
	}
}

func TestPriority(t *testing.T) {
	if p := (Sightings{Coauthors: 1, Tags: 1, Mentions: 1, Comments: 2, IraqContext: 1, Sources: 2}).Priority(); p != 3+2+2+2+2+6 {
		t.Fatalf("priority %v", p)
	}
}

// Influencers, as the owner asked (2026-11-03), at the default threshold
// (4). Linked once: one registered account tags them. Wide audience: ten
// registered accounts link to them, from posts located in Iraq.
func TestInfluencers(t *testing.T) {
	once := Sightings{Tags: 1, Sources: 1}
	wide := Sightings{Tags: 8, Mentions: 3, Comments: 6, IraqContext: 2, Sources: 10}
	for _, tc := range []struct {
		name       string
		vet        Vet
		once, wide bool
	}{
		{"Iraqi, Iraq in the bio", Vet{Biography: "Lifestyle | Baghdad 🇮🇶 | business 0770 123 4567"}, true, true},
		{"Iraqi, English bio, Iraqi dialect in captions", Vet{Biography: "content creator ✨",
			Captions: []string{"شلونكم حبايب", "هسه نزلت الفيديو", "كلش حلو اليوم"}}, true, true},
		{"Kurdish, Sorani", Vet{Biography: "دروستکەری ناوەڕۆک", Captions: []string{"ئێوارەیەکی جوان"}}, false, true},
		{"Iraqi, Arabic bio, no place", Vet{Biography: "صانعة محتوى ✨ للتعاون راسلوني"}, false, true},
		{"Iraqi, English only", Vet{Biography: "content creator ✨ fashion", Captions: []string{"new look", "coffee time"}}, false, true},
		{"Arab, no country named", Vet{Biography: "مدونة موضة وجمال", Captions: []string{"اطلالة اليوم"}}, false, true},
		{"Gulf, Dubai in the bio", Vet{Biography: "مدونة موضة | دبي 🇦🇪"}, false, false},
		{"global English star", Vet{Biography: "Athlete. Entrepreneur.", Followers: 40_000_000}, false, false},
	} {
		if got := Score(tc.vet, once).Sum() >= 4; got != tc.once {
			t.Errorf("%s, linked once: %v (%v)", tc.name, got, Score(tc.vet, once))
		}
		if got := Score(tc.vet, wide).Sum() >= 4; got != tc.wide {
			t.Errorf("%s, wide audience: %v (%v)", tc.name, got, Score(tc.vet, wide))
		}
	}
}

func TestInIraq(t *testing.T) {
	for _, tc := range []struct {
		name     string
		lat, lng *float64
		want     bool
	}{
		{"Baghdad, Iraq", nil, nil, true},
		{"مطعم في البصرة", nil, nil, true},
		{"Somewhere", f(33.31), f(44.36), true},
		{"Somewhere", f(51.5), f(-0.12), false},
		{"London", nil, nil, false},
	} {
		if got := InIraq(tc.name, tc.lat, tc.lng); got != tc.want {
			t.Errorf("%s %v %v: %v", tc.name, tc.lat, tc.lng, got)
		}
	}
}
