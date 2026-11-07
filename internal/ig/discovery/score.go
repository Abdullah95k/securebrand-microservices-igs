// Package discovery finds profiles to register (M11). Crawlers count the
// accounts they meet in the profiles igs reads (tagged, co-authors,
// mentioned, commenters: Collector); the leader scheduler vets the most
// seen with a profile-only read (the profile and its newest posts'
// captions, which a crawler records in ig.discovery_candidates and
// publishes nowhere), scores them for Iraqi signals (Score) and registers
// the public ones at ig.discovery.threshold or above for the schedule
// (Vetter). Every read is kept (ig.discovery_vets).
package discovery

import (
	"math"
	"regexp"
	"strings"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/keywords"
)

// places is the shared Iraq place list discovery matches (Iraq, the
// Kurdistan Region, the governorates and their towns): the embedded seed
// until UseKeywords swaps in the table's (ig.discovery_keywords).
var places atomic.Pointer[keywords.Matcher]

func init() {
	seed, err := keywords.Seed()
	if err != nil {
		panic(err) // the embedded seed; its tests parse it
	}
	places.Store(keywords.SeedMatcher(seed))
}

// UseKeywords makes discovery match m's places (keywords.Watch calls it
// with the table's enabled keywords).
func UseKeywords(m *keywords.Matcher) { places.Store(m) }

// KeywordsRefresh is how often the crawlers and the scheduler reload the
// place list, so an operator's change counts within minutes.
const KeywordsRefresh = 5 * time.Minute

// Place groups with a signal of their own; every other group is a
// governorate.
const (
	groupIraq      = "iraq"
	groupKurdistan = "kurdistan_region"
)

// Vet is what a vet read of a candidate found.
type Vet struct {
	Found     bool     `json:"found"`
	Private   bool     `json:"private,omitempty"`
	UserPK    int64    `json:"user_pk,omitempty"`
	Username  string   `json:"username,omitempty"`
	FullName  string   `json:"full_name,omitempty"`
	Biography string   `json:"biography,omitempty"`
	Category  string   `json:"category,omitempty"`
	Links     []string `json:"links,omitempty"`
	Followers int64    `json:"followers,omitempty"`
	Verified  bool     `json:"verified,omitempty"`
	Business  bool     `json:"business,omitempty"`
	// Captions of the newest posts (the grid's first page).
	Captions []string `json:"captions,omitempty"`
}

// Sightings is how often, and how, the crawl met a candidate.
type Sightings struct {
	Tags        int `json:"tags"`
	Coauthors   int `json:"coauthors"`
	Mentions    int `json:"mentions"`
	Comments    int `json:"comments"`
	IraqContext int `json:"iraq_context"`
	// Sources is how many different registered accounts it was met in:
	// its Iraqi audience.
	Sources int `json:"sources"`
}

// Priority weighs sightings for the vetting queue (ig.discovery.min_priority).
func (s Sightings) Priority() float64 {
	return float64(3*s.Coauthors + 2*s.Tags + 2*s.Mentions + s.Comments + 2*s.IraqContext + 3*s.Sources)
}

// GlobalFollowers: an account this large with nothing of Iraq of its own
// (a world star whom Iraqi fans tag) gets at most GlobalNetworkCap from
// network evidence.
const (
	GlobalFollowers  = 1_000_000
	GlobalNetworkCap = 2.0
	// NetworkCap bounds network evidence: alone it reaches the default
	// threshold (4) only with ten or more registered accounts linking to the
	// account and sightings on posts located in Iraq.
	NetworkCap = 4.0
)

// Signals are a score's parts, stored with it so an operator can see why a
// profile was or was not registered.
type Signals map[string]float64

// Sum is the score.
func (s Signals) Sum() float64 {
	var n float64
	for _, v := range s {
		n += v
	}
	return math.Round(n*100) / 100
}

// Score rates how Iraqi a candidate is, or how much it addresses an Iraqi
// audience. Its own words count most:
//   - Iraq named (any script, 🇮🇶): 3; an Iraqi phone 3 (+964 or 00964, or
//     07xx xxx xxxx, the Iraqi grouping), 1 for any other 07 number (an
//     unspaced 07700900123 is also a UK mobile's shape); an .iq link 2; a
//     place in an Iraqi governorate 2, 3 for two or more governorates (1
//     when only in captions); Kurdistan 1.5. Places are the shared keyword
//     list's (places, UseKeywords);
//   - Sorani Kurdish letters 1.5; Iraqi dialect 1, 2 for three or more
//     markers; mostly Arabic script 1;
//   - another country named without Iraq (or an Iraqi phone, .iq link or
//     place): -2.
//
// Network evidence (every candidate was met in registered, Iraqi, profiles)
// adds up to 4:
//   - iraqi_audience, how many different registered accounts tag, mention,
//     co-author with or comment as it: one 1, two 1.5, three 2, five 2.5,
//     ten or more 3;
//   - seen_in_iraq, sightings on posts located in Iraq: 1.5;
//   - coauthor, a collab post with a registered account: 0.5.
//
// So an Iraqi account that says nothing of Iraq itself (an English-only
// influencer) is added once ten registered accounts link to it from posts
// located in Iraq, and a foreign account once a real Iraqi audience
// surrounds it. An account with a million followers or more and nothing
// of Iraq of its own (a world star Iraqi fans tag) gets at most 2 from the
// network.
func Score(v Vet, s Sightings) Signals {
	sig := Signals{}
	own := scan(strings.Join(append([]string{v.FullName, v.Biography, v.Category}, v.Links...), "\n"))
	caps := scan(strings.Join(v.Captions, "\n"))
	if own.iraq || caps.iraq {
		sig["iraq_named"] = 3
	}
	switch {
	case own.phone:
		sig["iraq_phone"] = 3
	case own.localPhone:
		sig["iraq_phone"] = 1
	}
	if own.iqDomain {
		sig["iq_link"] = 2
	}
	switch cities := len(union(own.cities, caps.cities)); {
	case len(own.cities) > 0 && cities >= 2:
		sig["iraq_city"] = 3
	case len(own.cities) > 0:
		sig["iraq_city"] = 2
	case cities > 0:
		sig["iraq_city"] = 1
	}
	if own.kurdistan || caps.kurdistan {
		sig["kurdistan"] = 1.5
	}
	if own.sorani || caps.sorani {
		sig["sorani_kurdish"] = 1.5
	}
	switch d := own.dialect + caps.dialect; {
	case d >= 3:
		sig["iraqi_dialect"] = 2
	case d >= 1:
		sig["iraqi_dialect"] = 1
	}
	if arabicShare(own.letters+caps.letters, own.arabic+caps.arabic) >= 0.3 {
		sig["arabic_script"] = 1
	}
	// A bare 07 number is too weak to outweigh another country named.
	if (own.otherCountry || caps.otherCountry) && sig["iraq_named"] == 0 && !own.phone && sig["iq_link"] == 0 && sig["iraq_city"] == 0 {
		sig["other_country"] = -2
	}
	ownSignal := false
	for _, x := range sig {
		if x > 0 {
			ownSignal = true
		}
	}
	src := s.Sources
	if src == 0 && s.Tags+s.Coauthors+s.Mentions+s.Comments > 0 {
		src = 1 // met before sources were counted
	}
	network := 0.0
	add := func(k string, x float64) {
		sig[k] = x
		network += x
	}
	switch {
	case src >= 10:
		add("iraqi_audience", 3)
	case src >= 5:
		add("iraqi_audience", 2.5)
	case src >= 3:
		add("iraqi_audience", 2)
	case src == 2:
		add("iraqi_audience", 1.5)
	case src == 1:
		add("iraqi_audience", 1)
	}
	if s.IraqContext > 0 {
		add("seen_in_iraq", 1.5)
	}
	if s.Coauthors > 0 {
		add("coauthor", 0.5)
	}
	limit := NetworkCap
	if v.Followers >= GlobalFollowers && !ownSignal {
		limit = GlobalNetworkCap
	}
	if network > limit {
		sig["network_capped"] = limit - network
	}
	return sig
}

func arabicShare(letters, arabic int) float64 {
	if letters < 10 {
		return 0
	}
	return float64(arabic) / float64(letters)
}

func union(a, b map[string]bool) map[string]bool {
	out := map[string]bool{}
	for k := range a {
		out[k] = true
	}
	for k := range b {
		out[k] = true
	}
	return out
}

// InIraq reports whether a post's location is in Iraq: its name names Iraq
// or an Iraqi city, or its coordinates fall in Iraq's bounding box.
func InIraq(name string, lat, lng *float64) bool {
	if lat != nil && lng != nil && *lat >= 29.0 && *lat <= 37.4 && *lng >= 38.8 && *lng <= 48.6 {
		return true
	}
	t := scan(name)
	return t.iraq || len(t.cities) > 0 || t.kurdistan
}

type textSignals struct {
	iraq, phone, localPhone, iqDomain, kurdistan, sorani, otherCountry bool
	cities                                                             map[string]bool
	dialect                                                            int
	letters, arabic                                                    int
}

var (
	// An Iraqi number: +964 or 00964 (any spacing), or a mobile number
	// (07, then 7, 8, 5 or 9: Asiacell, Zain, Korek...) grouped the Iraqi
	// way, 4-3-4.
	phoneRe = regexp.MustCompile(`(\+|00)\s?964|(^|[^0-9])07[3-9][0-9][\s-][0-9]{3}[\s-][0-9]{4}([^0-9]|$)`)
	// Any other 07 mobile number, unspaced or grouped otherwise: other
	// countries' mobiles look the same (a UK 07700 900123).
	localPhoneRe = regexp.MustCompile(`(^|[^0-9])07[3-9][0-9][\s-]?[0-9]{3}[\s-]?[0-9]{4}([^0-9]|$)`)
	iqRe         = regexp.MustCompile(`(?i)\.iq(/|\b|$)`)
	digits       = strings.NewReplacer("٠", "0", "١", "1", "٢", "2", "٣", "3", "٤", "4", "٥", "5", "٦", "6", "٧", "7", "٨", "8", "٩", "9",
		"۰", "0", "۱", "1", "۲", "2", "۳", "3", "۴", "4", "۵", "5", "۶", "6", "۷", "7", "۸", "8", "۹", "9")
)

func scan(text string) textSignals {
	t := textSignals{cities: map[string]bool{}}
	if text == "" {
		return t
	}
	ascii := digits.Replace(text)
	t.phone = phoneRe.MatchString(ascii)
	t.localPhone = !t.phone && localPhoneRe.MatchString(ascii)
	t.iqDomain = iqRe.MatchString(text)
	if strings.Contains(text, "🇮🇶") {
		t.iraq = true
	}
	for _, f := range otherFlags {
		if strings.Contains(text, f) {
			t.otherCountry = true
		}
	}
	for _, r := range text {
		if !unicode.IsLetter(r) {
			continue
		}
		t.letters++
		if unicode.Is(unicode.Arabic, r) {
			t.arabic++
		}
		if strings.ContainsRune(soraniLetters, r) {
			t.sorani = true
		}
	}
	words := tokens(text)
	for i, w := range words {
		forms := variants(w)
		if i+1 < len(words) {
			forms = append(forms, variants(w+" "+words[i+1])...)
		}
		for _, f := range forms {
			if otherCountries[f] {
				t.otherCountry = true
			}
			if dialect[f] {
				t.dialect++
			}
		}
	}
	for g := range places.Load().Groups(text) {
		switch g {
		case groupIraq:
			t.iraq = true
		case groupKurdistan:
			t.kurdistan = true
		default:
			t.cities[g] = true
		}
	}
	return t
}

// tokens splits text into lower-case words of letters and digits, with
// Arabic letters normalised (alef forms, ta marbuta, alef maqsura, Persian
// and Kurdish kaf and yeh) and diacritics and tatweel dropped.
func tokens(text string) []string {
	var out []string
	var b strings.Builder
	flush := func() {
		if b.Len() > 0 {
			out = append(out, b.String())
			b.Reset()
		}
	}
	for _, r := range strings.ToLower(text) {
		switch {
		case unicode.Is(unicode.Mn, r) || r == 'ـ':
			continue
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			if n, ok := arabicFold[r]; ok {
				r = n
			}
			b.WriteRune(r)
		default:
			flush()
		}
	}
	flush()
	return out
}

var arabicFold = map[rune]rune{'أ': 'ا', 'إ': 'ا', 'آ': 'ا', 'ٱ': 'ا', 'ة': 'ه', 'ى': 'ي', 'ک': 'ك', 'ی': 'ي', 'ي': 'ي', 'ە': 'ه'}

// variants are the word and, for Arabic, the word without a leading
// conjunction, preposition or article: "وبغداد", "ببغداد" and "البصرة" all
// name a city.
func variants(w string) []string {
	out := []string{w}
	for _, p := range []string{"وبال", "وال", "بال", "لل", "ال", "و", "ب", "ل", "ف"} {
		if rest, ok := strings.CutPrefix(w, p); ok && len([]rune(rest)) >= 3 {
			out = append(out, rest)
			if r2, ok := strings.CutPrefix(rest, "ال"); ok && len([]rune(r2)) >= 3 {
				out = append(out, r2)
			}
		}
	}
	return out
}

// soraniLetters are letters of Kurdish (Sorani) in Arabic script that
// Arabic does not use: ڕ ڵ ێ ۆ ە.
const soraniLetters = "ڕڵێۆە"

var otherCountries = set("saudi", "ksa", "riyadh", "jeddah", "uae", "dubai", "abudhabi", "abu dhabi", "sharjah", "kuwait", "qatar", "doha",
	"bahrain", "oman", "muscat", "egypt", "cairo", "jordan", "amman", "syria", "damascus", "lebanon", "beirut", "iran", "tehran", "turkey",
	"istanbul", "morocco", "algeria", "tunisia", "libya", "yemen", "sudan", "palestine",
	"السعوديه", "الرياض", "جده", "الامارات", "دبي", "ابوظبي", "الشارقه", "الكويت", "قطر", "الدوحه", "البحرين", "عمان", "مسقط", "مصر",
	"القاهره", "الاردن", "سوريا", "دمشق", "لبنان", "بيروت", "ايران", "طهران", "تركيا", "اسطنبول", "المغرب", "الجزائر", "تونس", "ليبيا",
	"اليمن", "السودان", "فلسطين")

var otherFlags = []string{"🇸🇦", "🇦🇪", "🇰🇼", "🇶🇦", "🇧🇭", "🇴🇲", "🇪🇬", "🇯🇴", "🇸🇾", "🇱🇧", "🇮🇷", "🇹🇷", "🇲🇦", "🇩🇿", "🇹🇳", "🇱🇾", "🇾🇪", "🇸🇩", "🇵🇸"}

// dialect are words of Iraqi Arabic that other dialects rarely write.
var dialect = set("شلونك", "شلونكم", "شلونج", "ماكو", "اكو", "هسه", "كلش", "شكو", "اغاتي", "شكد", "وياك", "وياكم", "هواي")

func set(words ...string) map[string]bool {
	m := map[string]bool{}
	for _, w := range words {
		m[strings.Join(tokens(w), " ")] = true
	}
	return m
}
