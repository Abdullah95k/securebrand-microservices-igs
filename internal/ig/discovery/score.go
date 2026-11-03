// Package discovery finds profiles to register (M11). Crawlers count the
// accounts they meet in the profiles igs reads (tagged, co-authors,
// mentioned, commenters: Collector); the leader scheduler vets the most
// seen with a profile-only read (the profile and its newest posts'
// captions, which a crawler records in ig.discovery_candidates and
// publishes nowhere), scores them for Iraqi signals (Score) and registers
// the public ones at ig.discovery.threshold or above for the schedule
// (Vetter).
package discovery

import (
	"math"
	"regexp"
	"strings"
	"unicode"
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
	Tags, Coauthors, Mentions, Comments, IraqContext int
}

// Priority weighs sightings for the vetting queue (ig.discovery.min_priority).
func (s Sightings) Priority() float64 {
	return float64(3*s.Coauthors + 2*s.Tags + 2*s.Mentions + s.Comments + 2*s.IraqContext)
}

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
//   - Iraq named (any script, 🇮🇶): 3; an Iraqi phone (+964, 07xx) 3; an
//     .iq link 2; an Iraqi city or province 2, 3 for two or more (1 when
//     only in captions); Kurdistan 1.5;
//   - Sorani Kurdish letters 1.5; Iraqi dialect 1, 2 for three or more
//     markers; mostly Arabic script 1;
//   - another country named without Iraq: -2.
//
// How it was met adds up to 3: a co-author 1.5, a tag 1, a mention 0.5,
// three or more comments 0.5, sightings on posts located in Iraq 1.5. Every
// candidate was met in a registered profile, so this is the audience the
// profile shares; alone it is never enough at the default threshold (4).
func Score(v Vet, s Sightings) Signals {
	sig := Signals{}
	own := scan(strings.Join(append([]string{v.FullName, v.Biography, v.Category}, v.Links...), "\n"))
	caps := scan(strings.Join(v.Captions, "\n"))
	if own.iraq || caps.iraq {
		sig["iraq_named"] = 3
	}
	if own.phone {
		sig["iraq_phone"] = 3
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
	if (own.otherCountry || caps.otherCountry) && sig["iraq_named"] == 0 && sig["iraq_phone"] == 0 && sig["iq_link"] == 0 && sig["iraq_city"] == 0 {
		sig["other_country"] = -2
	}
	ctx := 0.0
	if s.Coauthors > 0 {
		ctx += 1.5
	}
	if s.Tags > 0 {
		ctx++
	}
	if s.Mentions > 0 {
		ctx += 0.5
	}
	if s.Comments >= 3 {
		ctx += 0.5
	}
	if s.IraqContext > 0 {
		ctx += 1.5
	}
	if ctx > 0 {
		sig["met_in_registered"] = math.Min(ctx, 3)
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
	iraq, phone, iqDomain, kurdistan, sorani, otherCountry bool
	cities                                                 map[string]bool
	dialect                                                int
	letters, arabic                                        int
}

var (
	// +964 or 00964 (any spacing), or an Iraqi mobile number: 07 then 7, 8,
	// 5 or 9 (Asiacell, Zain, Korek...), then 8 digits.
	phoneRe = regexp.MustCompile(`(\+|00)\s?964|(^|[^0-9])07[3-9][0-9][\s-]?[0-9]{3}[\s-]?[0-9]{4}([^0-9]|$)`)
	iqRe    = regexp.MustCompile(`(?i)\.iq(/|\b|$)`)
	digits  = strings.NewReplacer("٠", "0", "١", "1", "٢", "2", "٣", "3", "٤", "4", "٥", "5", "٦", "6", "٧", "7", "٨", "8", "٩", "9",
		"۰", "0", "۱", "1", "۲", "2", "۳", "3", "۴", "4", "۵", "5", "۶", "6", "۷", "7", "۸", "8", "۹", "9")
)

func scan(text string) textSignals {
	t := textSignals{cities: map[string]bool{}}
	if text == "" {
		return t
	}
	ascii := digits.Replace(text)
	t.phone = phoneRe.MatchString(ascii)
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
			switch {
			case iraqTerms[f]:
				t.iraq = true
			case kurdistanTerms[f]:
				t.kurdistan = true
			case otherCountries[f]:
				t.otherCountry = true
			}
			if c, ok := cities[f]; ok {
				t.cities[c] = true
			}
			if dialect[f] {
				t.dialect++
			}
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

var iraqTerms = set("iraq", "iraqi", "iraqis", "irak", "iraqia", "عراق", "عراقي", "عراقيه", "عراقيين", "العراق", "العراقي", "العراقيه",
	"عيراق", "عێراق", "عيراقي")

var kurdistanTerms = set("kurdistan", "krg", "كردستان", "کوردستان", "كوردستان", "kurdistanê")

var otherCountries = set("saudi", "ksa", "riyadh", "jeddah", "uae", "dubai", "abudhabi", "abu dhabi", "sharjah", "kuwait", "qatar", "doha",
	"bahrain", "oman", "muscat", "egypt", "cairo", "jordan", "amman", "syria", "damascus", "lebanon", "beirut", "iran", "tehran", "turkey",
	"istanbul", "morocco", "algeria", "tunisia", "libya", "yemen", "sudan", "palestine",
	"السعوديه", "الرياض", "جده", "الامارات", "دبي", "ابوظبي", "الشارقه", "الكويت", "قطر", "الدوحه", "البحرين", "عمان", "مسقط", "مصر",
	"القاهره", "الاردن", "سوريا", "دمشق", "لبنان", "بيروت", "ايران", "طهران", "تركيا", "اسطنبول", "المغرب", "الجزائر", "تونس", "ليبيا",
	"اليمن", "السودان", "فلسطين")

var otherFlags = []string{"🇸🇦", "🇦🇪", "🇰🇼", "🇶🇦", "🇧🇭", "🇴🇲", "🇪🇬", "🇯🇴", "🇸🇾", "🇱🇧", "🇮🇷", "🇹🇷", "🇲🇦", "🇩🇿", "🇹🇳", "🇱🇾", "🇾🇪", "🇸🇩", "🇵🇸"}

// cities maps the names of Iraq's cities and provinces (Latin, Arabic and
// Kurdish spellings, folded as tokens are) to one name each.
var cities = func() map[string]string {
	m := map[string]string{}
	for name, forms := range map[string][]string{
		"baghdad":      {"baghdad", "بغداد"},
		"basra":        {"basra", "basrah", "بصره", "البصره"},
		"mosul":        {"mosul", "موصل", "الموصل"},
		"erbil":        {"erbil", "arbil", "irbil", "hawler", "hewler", "hewlêr", "اربيل", "هەولێر", "ههولێر", "هولير"},
		"sulaymaniyah": {"sulaymaniyah", "sulaimani", "sulaymani", "slemani", "silemani", "سليمانيه", "السليمانيه", "سلێمانی", "سلێماني", "سليماني"},
		"duhok":        {"duhok", "dohuk", "dahuk", "دهوك", "دهۆک", "دهۆك"},
		"kirkuk":       {"kirkuk", "كركوك", "کەرکووک", "كهركووك"},
		"najaf":        {"najaf", "نجف", "النجف"},
		"karbala":      {"karbala", "kerbala", "كربلاء", "كربلا"},
		"nasiriyah":    {"nasiriyah", "nasiriya", "ناصريه", "الناصريه"},
		"hillah":       {"hillah", "hilla", "حله", "الحله"},
		"ramadi":       {"ramadi", "رمادي", "الرمادي"},
		"fallujah":     {"fallujah", "falluja", "فلوجه", "الفلوجه"},
		"samarra":      {"samarra", "سامراء"},
		"amarah":       {"amarah", "amara", "العماره"},
		"diwaniyah":    {"diwaniyah", "diwaniya", "ديوانيه", "الديوانيه"},
		"kut":          {"kut", "الكوت"},
		"baqubah":      {"baqubah", "baquba", "بعقوبه"},
		"tikrit":       {"tikrit", "تكريت"},
		"halabja":      {"halabja", "حلبجه", "هەڵەبجە", "ههڵهبجه"},
		"zakho":        {"zakho", "زاخو"},
		"kufa":         {"kufa", "الكوفه"},
		"anbar":        {"anbar", "انبار", "الانبار"},
		"diyala":       {"diyala", "ديالى", "ديالي"},
		"wasit":        {"wasit", "واسط"},
		"maysan":       {"maysan", "misan", "ميسان"},
		"dhi qar":      {"dhi qar", "dhiqar", "ذي قار", "ذيقار"},
		"muthanna":     {"muthanna", "samawah", "المثني", "السماوه"},
		"babylon":      {"babylon", "babil", "بابل"},
		"salahuddin":   {"salahuddin", "salah al din", "صلاح الدين"},
		"nineveh":      {"nineveh", "ninawa", "نينوي", "نينوى"},
		"qadisiyah":    {"qadisiyah", "القادسيه"},
		"zubair":       {"zubair", "الزبير"},
	} {
		for _, f := range forms {
			m[strings.Join(tokens(f), " ")] = name
		}
	}
	return m
}()

// dialect are words of Iraqi Arabic that other dialects rarely write.
var dialect = set("شلونك", "شلونكم", "شلونج", "ماكو", "اكو", "هسه", "كلش", "شكو", "اغاتي", "شكد", "وياك", "وياكم", "هواي")

func set(words ...string) map[string]bool {
	m := map[string]bool{}
	for _, w := range words {
		m[strings.Join(tokens(w), " ")] = true
	}
	return m
}
