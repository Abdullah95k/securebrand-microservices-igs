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
		{"two-word province and a city", Vet{Biography: "من ذي قار والناصرية"}, Sightings{}, Signals{"iraq_city": 3, "arabic_script": 1}},
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
		{"how it was met", Vet{}, Sightings{Coauthors: 1, Tags: 2, Mentions: 1, Comments: 5, IraqContext: 1}, Signals{"met_in_registered": 3}},
		{"a tag", Vet{}, Sightings{Tags: 1}, Signals{"met_in_registered": 1}},
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
// known only by how it was met do not.
func TestScoreAtTheDefaultThreshold(t *testing.T) {
	met := Sightings{Tags: 1}
	for _, tc := range []struct {
		name string
		vet  Vet
		pass bool
	}{
		{"photographer in Baghdad", Vet{FullName: "علي", Biography: "مصور فوتوغرافي | بغداد"}, true},
		{"shop with a phone", Vet{Biography: "ملابس اطفال توصيل لكل المحافظات 07701234567"}, true},
		{"Kurdish in Sulaymaniyah", Vet{Biography: "خۆشەویستی و ژیان لە سلێمانی"}, true},
		{"dialect in captions", Vet{Captions: []string{"شلونكم حبايب", "هسه نزلت الفيديو", "ماكو مثلكم"}}, true},
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
	if p := (Sightings{Coauthors: 1, Tags: 1, Mentions: 1, Comments: 2, IraqContext: 1}).Priority(); p != 3+2+2+2+2 {
		t.Fatalf("priority %v", p)
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
