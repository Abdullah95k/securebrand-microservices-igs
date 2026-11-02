package parse

import (
	"crypto/sha256"
	"sort"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"

	commonv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/common/v1"
	igv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/ig/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/walk"
)

// ProfileResult is one profile query answer.
type ProfileResult struct {
	Profile *igv1.Profile
	// UserID is the user object's id (17841…), which the grid and reels
	// queries take (not the pk).
	UserID string
	// Highlights is the profile's highlights list: id, title and cover;
	// items need their own query.
	Highlights []*igv1.Highlight
	// HighlightsHash changes when the list does, so items are read only
	// then (ADR 0005 §4).
	HighlightsHash []byte
	HasClips       bool
	// NotFound: the query answered with a null user (renamed, deleted).
	NotFound bool
	Media    []*igv1.Media
}

// userRoots are the root fields a profile answer has carried, newest first.
var userRoots = []string{"data.xig_user_by_username", "data.user", "data.xdt_user_by_username"}

// Profile parses a PolarisLoggedOutDesktopWWWProfileRootContentQuery answer.
func Profile(parts []gjson.Result) (ProfileResult, walk.Report) {
	var res ProfileResult
	var rep walk.Report
	var u gjson.Result
	nullRoot := false
	for _, p := range parts {
		for _, root := range userRoots {
			v := p.Get(root)
			if v.IsObject() {
				u = v
				break
			}
			if v.Exists() && v.Type == gjson.Null {
				nullRoot = true
			}
		}
		if u.Exists() {
			break
		}
	}
	if !u.Exists() {
		if nullRoot {
			res.NotFound = true
			return res, rep
		}
		rep.AddMissing("user")
		return res, rep
	}
	pk := walk.Int64(u.Get("pk"))
	if pk == 0 {
		rep.AddMissing("pk")
	}
	username := u.Get("username").String()
	if username == "" {
		rep.AddMissing("username")
	}
	res.UserID = u.Get("id").String()
	if res.UserID == "" {
		rep.AddOptional("id")
	}
	p := &igv1.Profile{
		UserPk: pk, Username: username, FullName: u.Get("full_name").String(), Biography: u.Get("biography").String(),
		IsVerified: u.Get("is_verified").Bool(), IsPrivate: u.Get("is_private").Bool(),
		Category: walk.First(u, "category_name", "category").String(), IsBusiness: u.Get("is_business_account").Bool(),
		Status: igv1.ProfileStatus_PROFILE_STATUS_ACTIVE,
	}
	if p.IsPrivate {
		p.Status = igv1.ProfileStatus_PROFILE_STATUS_PRIVATE
	}
	count := func(dst **int64, paths ...string) {
		if v := walk.First(u, paths...); v.Exists() && (v.Type == gjson.Number || v.Type == gjson.String) {
			n := walk.Int64(v)
			*dst = &n
			return
		}
		rep.AddOptional(paths[0])
	}
	count(&p.FollowerCount, "follower_count", "edge_followed_by.count")
	count(&p.FollowingCount, "following_count", "edge_follow.count")
	if v := walk.First(u, "media_count", "all_media_count", "edge_owner_to_timeline_media.count"); v.Type == gjson.Number {
		n := v.Int()
		p.MediaCount = &n
	}
	for _, l := range u.Get("bio_links").Array() {
		if url := walk.First(l, "url", "lynx_url").String(); url != "" {
			p.BioLinks = append(p.BioLinks, &igv1.BioLink{Url: url, Title: l.Get("title").String()})
		}
	}
	if fb := u.Get("linked_fb_info"); fb.IsObject() {
		p.LinkedFacebookPage = walk.First(fb, "linked_fb_page.name", "linked_fb_user.name", "name").String()
	}
	var pronouns []string
	for _, x := range u.Get("pronouns").Array() {
		pronouns = append(pronouns, x.String())
	}
	p.Pronouns = strings.Join(pronouns, "/")
	if pic := walk.First(u, "hd_profile_pic_url_info.url", "profile_pic_url_hd", "profile_pic_url").String(); pic != "" {
		p.ProfilePic = &commonv1.ImageRef{Url: pic}
		res.Media = append(res.Media, &igv1.Media{
			MediaId: MediaID("profile:"+strconv.FormatInt(pk, 10), "original"), SourceMediaId: "profile:" + strconv.FormatInt(pk, 10),
			Kind: commonv1.MediaKind_MEDIA_KIND_PROFILE_PIC, Variant: "original", OwnerType: "profile", UserPk: pk,
			SourceUrl: pic, SourceExpiresAt: walk.URLExpiry(pic),
		})
	} else {
		rep.AddOptional("profile_pic_url")
	}
	res.HasClips = u.Get("has_any_clips").Bool()
	hl := u.Get("lox_highlights_connection")
	if !hl.Exists() {
		hl = u.Get("edge_highlight_reels")
	}
	var ids []string
	for _, e := range hl.Get("edges").Array() {
		n := e.Get("node")
		raw := strings.TrimPrefix(n.Get("id").String(), "highlight:")
		id := walk.Int64(gjson.Parse(`"` + raw + `"`))
		if id == 0 {
			continue
		}
		h := &igv1.Highlight{HighlightId: id, UserPk: pk, Title: n.Get("title").String()}
		if c := walk.First(n, "cover_media_cropped_thumbnail_url", "cover_media.cropped_image_version.url", "cover_media.thumbnail_src").String(); c != "" {
			h.Cover = &commonv1.ImageRef{Url: c}
		}
		res.Highlights = append(res.Highlights, h)
		ids = append(ids, raw+"|"+h.Title)
	}
	n := int32(len(res.Highlights))
	p.HighlightCount = &n
	if len(ids) > 0 {
		sort.Strings(ids)
		sum := sha256.Sum256([]byte(strings.Join(ids, "\n")))
		res.HighlightsHash = sum[:16]
	}
	res.Profile = p
	return res, rep
}
