package parse

import (
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/types/known/timestamppb"

	igv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/ig/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/htmlx"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/walk"
)

// PostResult is a post's detail, read from its page.
type PostResult struct {
	Post  *igv1.Post
	Media []*igv1.Media
	// Comments are the first comments the page embeds (about a dozen), with
	// their connection's page_info: a post with few comments needs no
	// comment query at all.
	Comments CommentsResult
	// Gated: the page holds the post but not its detail (a login wall
	// drawn in place of it).
	Gated bool
}

// PostPage parses a post or reel page (/p/<code>/, /reel/<code>/). base is
// the site's base URL, for the permalink.
func PostPage(html []byte, shortcode, base string) (PostResult, walk.Report) {
	return PostParts(htmlx.JSONBlobs(html), shortcode, base)
}

// PostParts parses a post's detail from JSON parts: the page's embedded
// payloads, or a post query's answer (U2, if a cheap one exists).
func PostParts(parts []gjson.Result, shortcode, base string) (PostResult, walk.Report) {
	var res PostResult
	var rep walk.Report
	var m, holder gjson.Result
	for _, p := range parts {
		if m = mediaByCode(p, shortcode); m.Exists() {
			break
		}
	}
	for _, p := range parts {
		if holder = walk.Find(p, "xig_polaris_media"); holder.Exists() {
			break
		}
	}
	if !m.Exists() {
		if holder.IsObject() && holder.Get("if_not_gated_logged_out").Type == gjson.Null {
			res.Gated = true
		}
		rep.AddMissing("media")
		return res, rep
	}
	res.Post, res.Media = post(m, base, &rep)
	if holder.IsObject() {
		res.Comments, _ = comments([]gjson.Result{holder}, CommentContext{MediaPK: res.Post.MediaPk, OwnerPK: res.Post.OwnerPk, PostTakenAt: res.Post.TakenAt})
	}
	return res, rep
}

// mediaByCode finds the post's own media object: the one with its shortcode
// and a taken_at (a page also carries the owner's other posts, without
// detail). An empty code takes the first media object with a taken_at.
func mediaByCode(v gjson.Result, code string) gjson.Result {
	switch {
	case v.IsObject():
		if c := v.Get("code"); c.Exists() && (code == "" || c.String() == code) && v.Get("taken_at").Exists() && v.Get("pk").Exists() {
			return v
		}
		var hit gjson.Result
		v.ForEach(func(_, x gjson.Result) bool {
			if x.IsObject() || x.IsArray() {
				hit = mediaByCode(x, code)
			}
			return !hit.Exists()
		})
		return hit
	case v.IsArray():
		var hit gjson.Result
		v.ForEach(func(_, x gjson.Result) bool {
			hit = mediaByCode(x, code)
			return !hit.Exists()
		})
		return hit
	}
	return gjson.Result{}
}

var (
	hashtagRe = regexp.MustCompile(`#([\p{L}\p{N}_]+)`)
	mentionRe = regexp.MustCompile(`@([A-Za-z0-9._]+)`)
	dashRe    = regexp.MustCompile(`mediaPresentationDuration="PT(?:(\d+)H)?(?:(\d+)M)?(?:([\d.]+)S)?"`)
)

func post(m gjson.Result, base string, rep *walk.Report) (*igv1.Post, []*igv1.Media) {
	pkStr := strings.SplitN(m.Get("pk").String(), "_", 2)[0]
	pk, _ := strconv.ParseInt(pkStr, 10, 64)
	if pk == 0 {
		rep.AddMissing("pk")
	}
	code := m.Get("code").String()
	takenAt := walk.Unix(m.Get("taken_at"))
	if takenAt == nil {
		rep.AddMissing("taken_at")
	}
	p := &igv1.Post{
		MediaPk: pk, Shortcode: code, OwnerPk: walk.Int64(walk.First(m, "user.pk", "owner.pk")), TakenAt: takenAt,
		MediaType: igv1.MediaType(walk.Int64(m.Get("media_type"))), ProductType: m.Get("product_type").String(),
		Caption:       walk.First(m, "caption.text", "edge_media_to_caption.edges.0.node.text").String(),
		CaptionEdited: m.Get("caption_is_edited").Bool(), AccessibilityCaption: m.Get("accessibility_caption").String(),
		CountsHidden:       m.Get("like_and_view_counts_disabled").Bool(),
		CommentingDisabled: walk.First(m, "commenting_disabled_for_viewer", "comments_disabled").Bool(),
		DiscoveredVia:      igv1.DiscoveredVia_DISCOVERED_VIA_GRID, Status: igv1.PostStatus_POST_STATUS_VISIBLE,
		Width: int32(walk.First(m, "original_width", "dimensions.width").Int()), Height: int32(walk.First(m, "original_height", "dimensions.height").Int()),
		AiLabel: walk.First(m, "ai_label_info.attribution_label_text", "ai_label_info.label").String(),
	}
	if p.OwnerPk == 0 {
		rep.AddOptional("user.pk")
	}
	if v := m.Get("has_audio"); v.Exists() {
		b := v.Bool()
		p.HasAudio = &b
	}
	if !p.CountsHidden {
		if v := m.Get("like_count"); v.Type == gjson.Number {
			n := v.Int()
			p.LikeCount = &n
		} else {
			rep.AddOptional("like_count")
		}
	}
	if v := m.Get("comment_count"); v.Type == gjson.Number {
		n := v.Int()
		p.CommentCount = &n
	} else {
		rep.AddOptional("comment_count")
	}
	if v := walk.First(m, "play_count", "ig_play_count", "view_count"); v.Type == gjson.Number {
		n := v.Int()
		p.PlayCount = &n
	}
	seen := map[string]bool{}
	for _, h := range hashtagRe.FindAllStringSubmatch(p.Caption, -1) {
		if t := strings.ToLower(h[1]); !seen[t] {
			seen[t] = true
			p.Hashtags = append(p.Hashtags, t)
		}
	}
	for _, mm := range mentionRe.FindAllStringSubmatch(p.Caption, -1) {
		p.Mentions = append(p.Mentions, strings.ToLower(strings.TrimRight(mm[1], ".")))
	}
	if l := m.Get("location"); l.IsObject() {
		loc := &igv1.Location{Pk: walk.Int64(l.Get("pk")), Name: l.Get("name").String()}
		if v := l.Get("lat"); v.Type == gjson.Number {
			f := v.Float()
			loc.Lat = &f
		}
		if v := l.Get("lng"); v.Type == gjson.Number {
			f := v.Float()
			loc.Lng = &f
		}
		p.Location = loc
	}
	p.Usertags = usertags(m.Get("usertags.in"), 0)
	for _, c := range m.Get("coauthor_producers").Array() {
		p.Coauthors = append(p.Coauthors, &igv1.Coauthor{UserPk: walk.Int64(c.Get("pk")), Username: c.Get("username").String()})
	}
	if mi := m.Get("clips_metadata.music_info"); mi.IsObject() {
		p.Music = &igv1.Music{
			Title:   walk.First(mi, "music_asset_info.title", "title").String(),
			Artist:  walk.First(mi, "music_asset_info.display_artist", "display_artist").String(),
			AudioId: walk.First(mi, "music_asset_info.audio_cluster_id", "music_asset_info.audio_asset_id", "audio_asset_id").String(),
		}
	} else if snd := m.Get("clips_metadata.original_sound_info"); snd.IsObject() {
		p.Music = &igv1.Music{
			Title: snd.Get("original_audio_title").String(), Artist: snd.Get("ig_artist.username").String(),
			AudioId: walk.First(snd, "audio_asset_id", "audio_id").String(), OriginalAudio: true,
		}
	}
	for _, t := range m.Get("related_topic_pills").Array() {
		if n := walk.First(t, "topic_name", "name").String(); n != "" {
			p.Topics = append(p.Topics, n)
		}
	}
	p.VideoDurationMs = durationMS(m)
	if code != "" {
		kind := "p"
		if p.ProductType == "clips" {
			kind = "reel"
		}
		p.Permalink = strings.TrimRight(base, "/") + "/" + kind + "/" + code + "/"
	}
	mc := MediaCtx{UserPK: p.OwnerPk, MediaPK: pk, OwnerType: "post", PostTakenAt: takenAt, AltText: p.AccessibilityCaption}
	var media []*igv1.Media
	children := m.Get("carousel_media").Array()
	if len(children) == 0 {
		media = Renditions(m, mc)
	} else if cover := Renditions(m, mc); len(cover) > 0 {
		// The carousel's own cover image, when it has one apart from its
		// first child.
		media = append(media, cover...)
	}
	for i, c := range children {
		cpk := walk.Int64(gjson.Parse(`"` + strings.SplitN(c.Get("pk").String(), "_", 2)[0] + `"`))
		parent := pk
		cmc := mc
		cmc.MediaPK, cmc.ParentMediaPK, cmc.Position, cmc.AltText = cpk, &parent, int32(i), c.Get("accessibility_caption").String()
		cm := Renditions(c, cmc)
		child := &igv1.CarouselChild{MediaPk: cpk, Position: int32(i), MediaType: igv1.MediaType(walk.Int64(c.Get("media_type"))),
			AccessibilityCaption: cmc.AltText}
		for _, x := range cm {
			child.MediaIds = append(child.MediaIds, x.MediaId)
		}
		p.Children = append(p.Children, child)
		p.Usertags = append(p.Usertags, usertags(c.Get("usertags.in"), cpk)...)
		media = append(media, cm...)
	}
	for _, x := range media {
		p.MediaIds = append(p.MediaIds, x.MediaId)
	}
	if len(media) == 0 {
		rep.AddOptional("image_versions2")
	}
	return p, media
}

func usertags(in gjson.Result, child int64) []*igv1.UserTag {
	var out []*igv1.UserTag
	for _, t := range in.Array() {
		ut := &igv1.UserTag{UserPk: walk.Int64(t.Get("user.pk")), Username: t.Get("user.username").String(), ChildMediaPk: child}
		if pos := t.Get("position").Array(); len(pos) == 2 {
			x, y := pos[0].Float(), pos[1].Float()
			ut.X, ut.Y = &x, &y
		}
		if ut.UserPk != 0 || ut.Username != "" {
			out = append(out, ut)
		}
	}
	return out
}

// durationMS is a video's length: video_duration (seconds) or, as on the
// post pages the probe read, the DASH manifest's duration.
func durationMS(m gjson.Result) int64 {
	if v := m.Get("video_duration"); v.Type == gjson.Number && v.Float() > 0 {
		return int64(math.Round(v.Float() * 1000))
	}
	g := dashRe.FindStringSubmatch(m.Get("video_dash_manifest").String())
	if g == nil {
		return 0
	}
	h, _ := strconv.ParseFloat(g[1], 64)
	mi, _ := strconv.ParseFloat(g[2], 64)
	s, _ := strconv.ParseFloat(g[3], 64)
	return int64(math.Round((h*3600 + mi*60 + s) * 1000))
}

// pkTime is a timestamp from a media pk, or nil.
func pkTime(pk string) *timestamppb.Timestamp {
	if t := TakenAtFromPK(pk); !t.IsZero() {
		return timestamppb.New(t)
	}
	return nil
}
