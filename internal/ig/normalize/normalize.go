// Package normalize maps igs's platform events (ig.*.v1) to the shared
// content contract (content.*.v1, architecture §5): the normalizer of the
// Instagram collector. Storage, analysis and alerts read only content.*.
//
// Mapping:
//   - ig.profile.v1 -> content.account (with bio, website, profile links and
//     the linked Facebook page, for the entity layer) and a content.metrics
//     account sample (followers, following, posts);
//   - ig.post.v1 -> content.post (kind post, video or reel);
//   - ig.post_metrics.v1 -> content.metrics (target post);
//   - ig.comment.v1 -> content.comment and a content.metrics comment sample;
//   - ig.media.v1 -> content.media;
//   - ig.highlight.v1 -> a content.post of kind highlight per item read.
package normalize

import (
	"encoding/json"
	"fmt"
	"strconv"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/common/v1"
	contentv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/content/v1"
	igv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/ig/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/discovery"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/parse"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/topics"
)

// Platform is Instagram's name in the content contract.
const Platform = "instagram"

// Out is one content event to publish.
type Out struct {
	Topic string
	Key   string
	Msg   proto.Message
}

// Key is the record key of a content entity: platform and native id.
func Key(nativeID string) string { return Platform + ":" + nativeID }

// Record maps one record of an ig.* topic (base name) to content events.
// Topics it does not map give none.
func Record(topic string, value []byte) ([]Out, error) {
	var msg proto.Message
	switch topic {
	case topics.Profile:
		msg = &igv1.Profile{}
	case topics.Post:
		msg = &igv1.Post{}
	case topics.PostMetrics:
		msg = &igv1.PostMetrics{}
	case topics.Comment:
		msg = &igv1.Comment{}
	case topics.Media:
		msg = &igv1.Media{}
	case topics.Highlight:
		msg = &igv1.Highlight{}
	default:
		return nil, nil
	}
	if err := proto.Unmarshal(value, msg); err != nil {
		return nil, fmt.Errorf("normalize: %s: %w", topic, err)
	}
	return Map(msg), nil
}

// Map maps one ig.v1 message to content events.
func Map(m proto.Message) []Out {
	switch v := m.(type) {
	case *igv1.Profile:
		return profile(v)
	case *igv1.Post:
		return []Out{post(v)}
	case *igv1.PostMetrics:
		return []Out{postMetrics(v)}
	case *igv1.Comment:
		return comment(v)
	case *igv1.Media:
		if out, ok := media(v); ok {
			return []Out{out}
		}
	case *igv1.Highlight:
		return highlight(v)
	}
	return nil
}

func id(n int64) string { return strconv.FormatInt(n, 10) }

// envelope is the content envelope of an ig.v1 observation of native.
func envelope(o *commonv1.Observation, kind, native string, published *timestamppb.Timestamp) *contentv1.Envelope {
	e := &contentv1.Envelope{Platform: Platform, NativeId: native, PublishedAt: published}
	if o == nil {
		return e
	}
	// One content event per ig event, kind and entity (a highlight's items
	// share one ig event): a redelivered record maps to the same ids.
	e.EventId = kind + ":" + native + ":" + o.EventId
	e.ObservedAt, e.Version = o.ObservedAt, o.Version
	if e.Version == 0 && o.ObservedAt != nil {
		e.Version = o.ObservedAt.AsTime().UnixMicro()
	}
	e.Collector, e.CollectorVersion, e.RawRef = o.Collector, o.CollectorVersion, o.RawRef
	return e
}

func extras(m map[string]any) string {
	for k, v := range m {
		switch x := v.(type) {
		case nil:
			delete(m, k)
		case string:
			if x == "" {
				delete(m, k)
			}
		case bool:
			if !x {
				delete(m, k)
			}
		case []string:
			if len(x) == 0 {
				delete(m, k)
			}
		}
	}
	if len(m) == 0 {
		return ""
	}
	b, _ := json.Marshal(m)
	return string(b)
}

// links are the bio links' URLs.
func links(ls []*igv1.BioLink) []string {
	out := make([]string, 0, len(ls))
	for _, l := range ls {
		out = append(out, l.Url)
	}
	return out
}

func profile(p *igv1.Profile) []Out {
	native := id(p.UserPk)
	a := &contentv1.Account{
		Envelope: envelope(p.Obs, "account", native, nil),
		Handle:   p.Username, Name: p.FullName, Kind: "profile", Verified: p.IsVerified, Category: p.Category,
		Bio: p.Biography, Followers: p.FollowerCount, Following: p.FollowingCount, PostCount: p.MediaCount,
		IsPrivate:  p.IsPrivate || p.Status == igv1.ProfileStatus_PROFILE_STATUS_PRIVATE,
		IsBusiness: p.IsBusiness,
		IsDeleted:  p.Status == igv1.ProfileStatus_PROFILE_STATUS_UNAVAILABLE || p.Status == igv1.ProfileStatus_PROFILE_STATUS_SUPPRESSED,
	}
	if p.ProfilePic != nil {
		a.PictureUrl = p.ProfilePic.Url
	}
	for _, l := range p.BioLinks {
		a.Links = append(a.Links, &contentv1.Link{Url: l.Url, Title: l.Title})
	}
	// The website: external_url when served, else the first bio link, as
	// the logged-out answer has carried it (REQ-008 fix 18).
	switch {
	case p.ExternalUrl != "":
		a.Website = p.ExternalUrl
	case len(a.Links) > 0:
		a.Website = a.Links[0].Url
	}
	a.CountrySignals = discovery.CountrySignals(p.FullName, p.Biography, p.Category, a.Website, links(p.BioLinks))
	// The linked Facebook page: its name as the title and, with its ID, the
	// page as platform "fb" (REQ-008 fix 9). The name is not a URL.
	if p.LinkedFacebookPage != "" || p.LinkedFacebookPageId != "" {
		l := &contentv1.Link{Title: p.LinkedFacebookPage, Platform: "fb", PlatformId: p.LinkedFacebookPageId}
		if p.LinkedFacebookPageId != "" {
			l.Url = "https://www.facebook.com/" + p.LinkedFacebookPageId
		}
		a.Links = append(a.Links, l)
	}
	var highlights *int32
	if p.HighlightCount != nil {
		highlights = p.HighlightCount
	}
	a.ExtrasJson = extras(map[string]any{"pronouns": p.Pronouns, "highlight_count": highlights, "requested_ref": p.RequestedRef,
		"url": profileURL(p.Username)})
	out := []Out{{Topic: topics.ContentAccount, Key: Key(native), Msg: a}}
	if p.FollowerCount != nil || p.FollowingCount != nil || p.MediaCount != nil {
		out = append(out, Out{Topic: topics.ContentMetrics, Key: Key(native), Msg: &contentv1.Metrics{
			Envelope: envelope(p.Obs, "account_metrics", native, nil), Target: "account",
			Followers: p.FollowerCount, Following: p.FollowingCount, PostCount: p.MediaCount,
		}})
	}
	return out
}

func profileURL(username string) string {
	if username == "" {
		return ""
	}
	return "https://www.instagram.com/" + username + "/"
}

func post(p *igv1.Post) Out {
	native := id(p.MediaPk)
	url := p.Permalink
	if url == "" && p.Shortcode != "" {
		url = "https://www.instagram.com/p/" + p.Shortcode + "/"
	}
	c := &contentv1.Post{
		Envelope:  envelope(p.Obs, "post", native, p.TakenAt),
		AccountId: id(p.OwnerPk), Kind: parse.PostKind(p), Text: p.Caption, Url: url,
		Views: p.PlayCount, Comments: p.CommentCount, MediaCount: int32(max(1, len(p.Children))),
		// A walled post may still exist: it is published as unavailable,
		// not deleted, with what the grid gave and no counts (REQ-008 fix 15).
		IsDeleted: p.Status == igv1.PostStatus_POST_STATUS_UNAVAILABLE && p.StatusReason != "login_wall",
	}
	if !p.CountsHidden {
		c.Likes = p.LikeCount
	}
	for _, co := range p.Coauthors {
		if co.UserPk != 0 && co.UserPk != p.OwnerPk {
			c.CoauthorIds = append(c.CoauthorIds, id(co.UserPk))
		}
	}
	var tagged []string
	for _, t := range p.Usertags {
		if t.UserPk != 0 {
			tagged = append(tagged, id(t.UserPk))
		}
	}
	x := map[string]any{"shortcode": p.Shortcode, "product_type": p.ProductType, "hashtags": p.Hashtags, "mentions": p.Mentions,
		"tagged_ids": tagged, "topics": p.Topics, "ai_label": p.AiLabel, "caption_edited": p.CaptionEdited, "counts_hidden": p.CountsHidden,
		"commenting_disabled": p.CommentingDisabled, "accessibility_caption": p.AccessibilityCaption,
		"discovered_via": enumName(p.DiscoveredVia.String(), "DISCOVERED_VIA_")}
	if p.Location != nil {
		x["location"] = map[string]any{"id": id(p.Location.Pk), "name": p.Location.Name, "lat": p.Location.Lat, "lng": p.Location.Lng}
	}
	if p.Music != nil {
		x["music"] = map[string]any{"title": p.Music.Title, "artist": p.Music.Artist, "audio_id": p.Music.AudioId, "original": p.Music.OriginalAudio}
	}
	if p.IsPinned != nil {
		x["is_pinned"] = *p.IsPinned
	}
	if p.VideoDurationMs > 0 {
		x["video_duration_ms"] = p.VideoDurationMs
	}
	if p.Status == igv1.PostStatus_POST_STATUS_UNAVAILABLE {
		x["status"] = "unavailable"
		if p.StatusReason != "" {
			x["status_reason"] = p.StatusReason
		}
	}
	c.ExtrasJson = extras(x)
	return Out{Topic: topics.ContentPost, Key: Key(native), Msg: c}
}

func enumName(s, prefix string) string {
	if len(s) > len(prefix) && s[:len(prefix)] == prefix {
		s = s[len(prefix):]
	}
	if s == "UNSPECIFIED" {
		return ""
	}
	b := []byte(s)
	for i, ch := range b {
		if ch >= 'A' && ch <= 'Z' {
			b[i] = ch + 'a' - 'A'
		}
	}
	return string(b)
}

func postMetrics(m *igv1.PostMetrics) Out {
	native := id(m.MediaPk)
	e := envelope(m.Obs, "post_metrics", native, m.TakenAt)
	if m.CapturedAt != nil {
		e.ObservedAt = m.CapturedAt
	}
	c := &contentv1.Metrics{Envelope: e, Target: "post", ParentId: id(m.OwnerPk), Views: m.PlayCount, Comments: m.CommentCount,
		CountsHidden: m.CountsHidden}
	if !m.CountsHidden {
		c.Likes = m.LikeCount
	}
	return Out{Topic: topics.ContentMetrics, Key: Key(native), Msg: c}
}

func comment(c *igv1.Comment) []Out {
	native := id(c.CommentPk)
	out := &contentv1.Comment{
		Envelope: envelope(c.Obs, "comment", native, c.CreatedAt),
		PostId:   id(c.MediaPk), AccountId: id(c.OwnerPk), Text: c.Text, Likes: c.LikeCount, Replies: c.ChildCommentCount,
		IsDeleted:  c.Status == igv1.CommentStatus_COMMENT_STATUS_DELETED,
		ExtrasJson: extras(map[string]any{"is_op": c.IsOp, "redacted": c.Redacted}),
	}
	if c.ParentCommentPk != nil {
		out.ParentCommentId = id(*c.ParentCommentPk)
	}
	if a := c.Author; a != nil {
		au := &contentv1.Author{Name: a.Username, ProfileUrl: profileURL(a.Username), PictureUrl: a.ProfilePicUrl,
			Verified: a.IsVerified, AuthorKey: a.AuthorKey}
		if a.UserPk != 0 {
			au.AuthorId = id(a.UserPk)
		}
		out.Author = au
	}
	res := []Out{{Topic: topics.ContentComment, Key: Key(id(c.MediaPk)), Msg: out}}
	if c.LikeCount != nil || c.ChildCommentCount != nil {
		res = append(res, Out{Topic: topics.ContentMetrics, Key: Key(native), Msg: &contentv1.Metrics{
			Envelope: envelope(c.Obs, "comment_metrics", native, c.CreatedAt), Target: "comment", ParentId: id(c.MediaPk),
			Likes: c.LikeCount, Replies: c.ChildCommentCount,
		}})
	}
	return res
}

func media(m *igv1.Media) (Out, bool) {
	var owner string
	switch m.OwnerType {
	case "comment":
		if m.CommentPk == nil {
			return Out{}, false
		}
		owner = id(*m.CommentPk)
	case "profile":
		owner = id(m.UserPk)
	default: // post, highlight
		owner = id(m.MediaPk)
		if m.ParentMediaPk != nil {
			owner = id(*m.ParentMediaPk) // a carousel child belongs to its post
		}
	}
	ownerType := m.OwnerType
	switch ownerType {
	case "profile":
		ownerType = "account"
	case "highlight", "":
		ownerType = "post"
	}
	typ := "image"
	if m.Kind == commonv1.MediaKind_MEDIA_KIND_VIDEO || m.Kind == commonv1.MediaKind_MEDIA_KIND_REEL {
		typ = "video"
	}
	return Out{Topic: topics.ContentMedia, Key: Key(m.MediaId), Msg: &contentv1.Media{
		Envelope:  envelope(m.Obs, "media", m.MediaId, m.PostTakenAt),
		OwnerType: ownerType, OwnerId: owner, Type: typ, Url: m.SourceUrl, UrlExpiresAt: m.SourceExpiresAt,
		Width: m.Width, Height: m.Height, DurationMs: m.DurationMs, AltText: m.AltText, Position: m.Position,
	}}, true
}

func highlight(h *igv1.Highlight) []Out {
	var out []Out
	link := "https://www.instagram.com/stories/highlights/" + id(h.HighlightId) + "/"
	for _, it := range h.Items {
		native := id(it.ItemPk)
		taken := it.TakenAt
		if taken == nil {
			if t := parse.TakenAtFromPK(native); !t.IsZero() {
				taken = timestamppb.New(t)
			}
		}
		kind := "image"
		if it.MediaType == 2 {
			kind = "video"
		}
		out = append(out, Out{Topic: topics.ContentPost, Key: Key(native), Msg: &contentv1.Post{
			Envelope:  envelope(h.Obs, "highlight_item", native, taken),
			AccountId: id(h.UserPk), Kind: parse.KindHighlight, Url: link, MediaCount: int32(max(1, len(it.MediaIds))),
			ExtrasJson: extras(map[string]any{"highlight_id": id(h.HighlightId), "highlight_title": h.Title, "media": kind,
				"media_ids": it.MediaIds}),
		}})
	}
	return out
}
