// Package normalize maps igs's platform events (ig.*.v1) to the SecureBrand
// unified contract (github.com/abdullah95k/securebrand-contracts, v1.2.0):
// the normalizer of the Instagram collector. Storage, analysis and alerts
// read only these (REQ-019).
//
// Mapping (slice 1, docs/master/igs-contract-adoption.md):
//   - ig.profile.v1 -> content.account (handle, name, bio with spans, links
//     with the linked Facebook page, verified, private, Iraq signals) and
//     content.account_snapshot (followers, following, posts);
//   - ig.post.v1 -> content.post (photo, carousel, video, igtv, reel);
//   - ig.post_metrics.v1 -> content.metrics (target post);
//   - ig.comment.v1 -> content.comment and content.metrics (target comment).
//
// Media, highlights, audio, discovery candidates and coverage follow in
// later slices; their ig.* records map to nothing until then.
package normalize

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"unicode"

	"golang.org/x/net/publicsuffix"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	contentv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/content/v1"

	commonv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/common/v1"
	igv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/ig/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/discovery"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/topics"
)

// Platform is Instagram's code in the contract's registry.
const Platform = "ig"

// The message kinds of the envelope.
const (
	KindAccount         = "account"
	KindAccountSnapshot = "account_snapshot"
	KindPost            = "post"
	KindComment         = "comment"
	KindMetrics         = "metrics"
	KindMedia           = "media"
	KindPlaylist        = "playlist"
	KindAudio           = "audio"
)

// Out is one content event to publish.
type Out struct {
	Topic string
	Key   string
	Msg   proto.Message
}

// ID is a contract ID: "ig:<entity kind>:<native id>".
func ID(entity, native string) string { return Platform + ":" + entity + ":" + native }

func pk(n int64) string { return strconv.FormatInt(n, 10) }

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
		return post(v)
	case *igv1.PostMetrics:
		return postMetrics(v)
	case *igv1.Comment:
		return comment(v)
	case *igv1.Media:
		return media(v)
	case *igv1.Highlight:
		return playlist(v)
	}
	return nil
}

var hashOpts = proto.MarshalOptions{Deterministic: true}

// contentHash is the hex SHA-256 of a message's content fields: m with its
// envelope and counts cleared (README §1: "not counts, not bookkeeping").
// Metrics, whose content is its counts, hash with them.
func contentHash(m proto.Message) string {
	c := proto.Clone(m)
	switch v := c.(type) {
	case *contentv1.Account:
		v.Envelope, v.Counts = nil, nil
	case *contentv1.AccountSnapshot:
		v.Envelope, v.Counts = nil, nil
	case *contentv1.Post:
		v.Envelope, v.Counts = nil, nil
	case *contentv1.Comment:
		v.Envelope, v.Counts = nil, nil
	case *contentv1.Metrics:
		v.Envelope = nil
	case *contentv1.Media:
		v.Envelope = nil
		signedURL(v)
	}
	b, _ := hashOpts.Marshal(c)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// seal sets the envelope's hash, version and event ID once the message is
// complete: event_id is the SHA-256 of "<id>|<observed_at µs>|<hash>", the
// contract's recommendation, so a redelivered observation keeps its ID.
func seal(env *contentv1.Envelope, m proto.Message) {
	env.ContentHash = contentHash(m)
	us := int64(0)
	if env.ObservedAt != nil {
		us = env.ObservedAt.AsTime().UnixMicro()
	}
	if env.Version == 0 {
		env.Version = us
	}
	sum := sha256.Sum256([]byte(env.Id + "|" + strconv.FormatInt(us, 10) + "|" + env.ContentHash))
	env.EventId = hex.EncodeToString(sum[:])
}

// envelope is the contract envelope of an ig.v1 observation; seal finishes
// it. kind is the message kind, entity and native the entity it is about.
func envelope(o *commonv1.Observation, kind, entity, native string) *contentv1.Envelope {
	e := &contentv1.Envelope{Platform: Platform, Kind: kind, Id: ID(entity, native), NativeId: native,
		Provenance: &contentv1.Provenance{Collector: "igs", Acquisition: contentv1.Acquisition_ACQUISITION_SCRAPED_LOGGED_OUT,
			ParserVersion: kind + ":0"}}
	if o == nil {
		return e
	}
	e.ObservedAt, e.Version = o.ObservedAt, o.Version
	pv := e.Provenance
	pv.CollectorVersion, pv.RawRef = o.CollectorVersion, o.RawRef
	if o.Collector != "" {
		pv.Collector = o.Collector
	}
	query := o.QueryName
	if query == "" {
		query = kind
	}
	// "<query>:<n>" (REQ-019 item 2): the parser that read it, so a parser
	// fix can be re-applied to the kept raw responses.
	pv.ParserVersion = query + ":" + strconv.FormatUint(uint64(o.ParserVersion), 10)
	pv.SourceOperation, pv.SourceVariant, pv.JobIds = o.QueryName, o.DocId, o.JobIds
	switch o.Mode {
	case commonv1.CrawlMode_CRAWL_MODE_FULL:
		pv.Mode = contentv1.CrawlMode_CRAWL_MODE_FULL
	case commonv1.CrawlMode_CRAWL_MODE_INCREMENTAL:
		pv.Mode = contentv1.CrawlMode_CRAWL_MODE_INCREMENTAL
	case commonv1.CrawlMode_CRAWL_MODE_REFRESH:
		pv.Mode = contentv1.CrawlMode_CRAWL_MODE_REFRESH
	case commonv1.CrawlMode_CRAWL_MODE_RECRAWL:
		pv.Mode = contentv1.CrawlMode_CRAWL_MODE_RECRAWL
	}
	return e
}

// exact is a count Instagram gave as a full number.
func exact(v *int64) *contentv1.Count {
	if v == nil {
		return nil
	}
	return &contentv1.Count{Value: proto.Int64(*v), Exactness: contentv1.CountExactness_COUNT_EXACTNESS_EXACT}
}

// exactTime is a time Instagram gave to the second.
func exactTime(t *timestamppb.Timestamp) *contentv1.Time {
	if t == nil {
		return nil
	}
	return &contentv1.Time{At: t, Precision: contentv1.TimePrecision_TIME_PRECISION_EXACT}
}

// extras keeps the non-empty values under their documented keys
// (registry/extras.yaml).
func extras(m map[string]any) *structpb.Struct {
	for k, v := range m {
		switch x := v.(type) {
		case nil:
			delete(m, k)
		case string:
			if x == "" {
				delete(m, k)
			}
		case []any:
			if len(x) == 0 {
				delete(m, k)
			}
		}
	}
	if len(m) == 0 {
		return nil
	}
	s, err := structpb.NewStruct(m)
	if err != nil {
		return nil
	}
	return s
}

// ProfileURL is an account's page.
func ProfileURL(username string) string {
	if username == "" {
		return ""
	}
	return "https://www.instagram.com/" + username + "/"
}

// accountRef is a reference to an Instagram account.
func accountRef(userPk int64, username string) *contentv1.AccountRef {
	r := &contentv1.AccountRef{Handle: username, Url: ProfileURL(username)}
	if userPk != 0 {
		r.NativeId = pk(userPk)
		r.Id = ID("account", r.NativeId)
	}
	return r
}

// Text is text exactly as received, with its hashtags and mentions as
// spans counted in code points.
func Text(s string) *contentv1.Text {
	if s == "" {
		return nil
	}
	return &contentv1.Text{Text: s, Spans: spans(s)}
}

func hashtagRune(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.Is(unicode.Mn, r)
}

func handleRune(r rune) bool {
	return r == '_' || r == '.' || ('a' <= r && r <= 'z') || ('A' <= r && r <= 'Z') || ('0' <= r && r <= '9')
}

// spans finds #hashtags and @mentions as Instagram links them: a # or @
// at the start or after a character that isn't part of a word, followed by
// a hashtag's letters, digits, marks and underscores, or a username's
// letters, digits, dots and underscores (a trailing dot ends the sentence).
func spans(s string) []*contentv1.TextSpan {
	rs := []rune(s)
	var out []*contentv1.TextSpan
	for i := 0; i < len(rs); i++ {
		if rs[i] != '#' && rs[i] != '@' {
			continue
		}
		if i > 0 && (hashtagRune(rs[i-1]) || rs[i-1] == '@' || rs[i-1] == '#') {
			continue
		}
		in := hashtagRune
		if rs[i] == '@' {
			in = handleRune
		}
		j := i + 1
		for j < len(rs) && in(rs[j]) {
			j++
		}
		if rs[i] == '@' {
			for j > i+1 && rs[j-1] == '.' {
				j--
			}
		}
		if j == i+1 {
			continue
		}
		sp := &contentv1.TextSpan{Offset: int32(i), Length: int32(j - i), Unit: contentv1.OffsetUnit_OFFSET_UNIT_CODE_POINT,
			Value: string(rs[i+1 : j])}
		if rs[i] == '#' {
			sp.Type = contentv1.SpanType_SPAN_TYPE_HASHTAG
		} else {
			sp.Type = contentv1.SpanType_SPAN_TYPE_MENTION
			sp.Account = &contentv1.AccountRef{Handle: sp.Value, Url: ProfileURL(sp.Value)}
		}
		out = append(out, sp)
		i = j - 1
	}
	return out
}

// platformHosts names the platforms a link can point at, by registrable
// domain (registry/platforms.yaml codes).
var platformHosts = map[string]string{
	"facebook.com": "fb", "fb.com": "fb", "fb.me": "fb", "instagram.com": "ig", "tiktok.com": "tt",
	"youtube.com": "yt", "youtu.be": "yt", "x.com": "x", "twitter.com": "x", "t.me": "tg", "telegram.me": "tg",
	"threads.net": "th", "threads.com": "th", "linkedin.com": "li", "snapchat.com": "sc",
}

// link describes a URL: its registrable domain and, for a known platform,
// that platform's code.
func link(raw, title string, source contentv1.LinkSource) *contentv1.Link {
	l := &contentv1.Link{Url: raw, Title: title, Source: source}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		u, err = url.Parse("https://" + raw)
	}
	if err == nil && u.Host != "" {
		host := strings.ToLower(strings.TrimPrefix(u.Hostname(), "www."))
		if d, err := publicsuffix.EffectiveTLDPlusOne(host); err == nil {
			l.Domain = d
		} else {
			l.Domain = host
		}
		l.TargetPlatform = platformHosts[l.Domain]
	}
	return l
}

func profile(p *igv1.Profile) []Out {
	native := pk(p.UserPk)
	if p.Status == igv1.ProfileStatus_PROFILE_STATUS_SUPPRESSED {
		// Erased: igs keeps nothing of it, and publishes nothing more.
		return nil
	}
	env := envelope(p.Obs, KindAccount, "account", native)
	if p.Status == igv1.ProfileStatus_PROFILE_STATUS_UNAVAILABLE {
		// Gone, renamed away or refused: only the status is known.
		env.Partial = true
		a := &contentv1.Account{Envelope: env, Status: &contentv1.Status{
			Availability: contentv1.Availability_AVAILABILITY_UNAVAILABLE, Reason: "no_user", Since: env.ObservedAt}}
		seal(env, a)
		return []Out{{Topic: topics.ContentAccount, Key: env.Id, Msg: a}}
	}
	private := p.IsPrivate || p.Status == igv1.ProfileStatus_PROFILE_STATUS_PRIVATE
	a := &contentv1.Account{
		Envelope: env, Handle: p.Username, DisplayName: p.FullName, Url: ProfileURL(p.Username), Bio: Text(p.Biography),
		Verified: proto.Bool(p.IsVerified), IsPrivate: proto.Bool(private),
		// Instagram's account type and business flag aren't known logged
		// out: unset, never guessed (REQ-019 item 3).
		Status: &contentv1.Status{Availability: contentv1.Availability_AVAILABILITY_VISIBLE},
	}
	if private {
		a.Status.Availability = contentv1.Availability_AVAILABILITY_PRIVATE
	}
	if p.Category != "" {
		a.Categories = []string{p.Category}
	}
	var urls []string
	for _, l := range p.BioLinks {
		a.Links = append(a.Links, link(l.Url, l.Title, contentv1.LinkSource_LINK_SOURCE_BIO))
		urls = append(urls, l.Url)
	}
	if p.ExternalUrl != "" && !containsURL(urls, p.ExternalUrl) {
		a.Links = append(a.Links, link(p.ExternalUrl, "", contentv1.LinkSource_LINK_SOURCE_WEBSITE_FIELD))
	}
	// The linked Facebook page (README's mapping table): its name as the
	// title and, with its ID, the page's URL.
	if p.LinkedFacebookPage != "" || p.LinkedFacebookPageId != "" {
		l := &contentv1.Link{Title: p.LinkedFacebookPage, Source: contentv1.LinkSource_LINK_SOURCE_LINKED_ACCOUNT,
			TargetPlatform: "fb", TargetId: p.LinkedFacebookPageId}
		if p.LinkedFacebookPageId != "" {
			l.Url, l.Domain = "https://www.facebook.com/"+p.LinkedFacebookPageId, "facebook.com"
		}
		a.Links = append(a.Links, l)
	}
	website := p.ExternalUrl
	if website == "" && len(urls) > 0 {
		website = urls[0]
	}
	if sig := discovery.CountrySignals(p.FullName, p.Biography, p.Category, website, urls); len(sig) > 0 {
		a.Relevance = &contentv1.Relevance{MatchedRules: sig}
	}
	x := map[string]any{"ig.pronouns": p.Pronouns, "ig.requested_ref": p.RequestedRef}
	if p.HighlightCount != nil {
		x["ig.highlight_count"] = float64(*p.HighlightCount)
	}
	a.Extras = extras(x)
	seal(env, a)
	out := []Out{{Topic: topics.ContentAccount, Key: env.Id, Msg: a}}
	if p.FollowerCount != nil || p.FollowingCount != nil || p.MediaCount != nil {
		// The account's counts go to snapshots, which change far more often
		// than the account (README, closed in v0.2).
		senv := envelope(p.Obs, KindAccountSnapshot, "account", native)
		s := &contentv1.AccountSnapshot{Envelope: senv, Handle: p.Username, DisplayName: p.FullName,
			Verified: a.Verified, IsPrivate: a.IsPrivate, Status: a.Status,
			Counts: &contentv1.AccountCounts{Followers: exact(p.FollowerCount), Following: exact(p.FollowingCount),
				Posts: exact(p.MediaCount)}}
		seal(senv, s)
		out = append(out, Out{Topic: topics.ContentAccountSnapshot, Key: env.Id, Msg: s})
	}
	return out
}

func containsURL(urls []string, u string) bool {
	for _, x := range urls {
		if strings.TrimRight(x, "/") == strings.TrimRight(u, "/") {
			return true
		}
	}
	return false
}

// postType is the contract's type and subtype (README, "Map by these
// rules"): photo and carousel are posts, a feed video or IGTV a video, a
// reel a short video.
func postType(p *igv1.Post) (contentv1.PostType, string) {
	switch {
	case p.ProductType == "clips":
		return contentv1.PostType_POST_TYPE_SHORT_VIDEO, "reel"
	case p.ProductType == "igtv":
		return contentv1.PostType_POST_TYPE_VIDEO, "igtv"
	case p.MediaType == igv1.MediaType_MEDIA_TYPE_VIDEO:
		return contentv1.PostType_POST_TYPE_VIDEO, "video"
	case p.MediaType == igv1.MediaType_MEDIA_TYPE_CAROUSEL:
		return contentv1.PostType_POST_TYPE_POST, "carousel"
	}
	return contentv1.PostType_POST_TYPE_POST, "photo"
}

// PostURL is a post's page.
func PostURL(p *igv1.Post) string {
	switch {
	case p.Permalink != "":
		return p.Permalink
	case p.Shortcode == "":
		return ""
	case p.ProductType == "clips":
		return "https://www.instagram.com/reel/" + p.Shortcode + "/"
	}
	return "https://www.instagram.com/p/" + p.Shortcode + "/"
}

// postCounts are a post's counts; a hidden like count has no value.
func postCounts(likes, comments, plays *int64, hidden bool) *contentv1.Engagement {
	e := &contentv1.Engagement{Comments: exact(comments), Views: exact(plays)}
	if hidden {
		e.Likes = &contentv1.Count{Hidden: true}
	} else {
		e.Likes = exact(likes)
	}
	if e.Likes == nil && e.Comments == nil && e.Views == nil {
		return nil
	}
	return e
}

func post(p *igv1.Post) []Out {
	native := pk(p.MediaPk)
	env := envelope(p.Obs, KindPost, "post", native)
	owner := ID("account", pk(p.OwnerPk))
	typ, sub := postType(p)
	c := &contentv1.Post{Envelope: env, AccountId: owner, Type: typ, Subtype: sub, Url: PostURL(p), PublishedAt: exactTime(p.TakenAt),
		Account: &contentv1.AccountRef{Id: owner, NativeId: pk(p.OwnerPk)}}
	if p.Status == igv1.PostStatus_POST_STATUS_UNAVAILABLE {
		// A re-read that found the post gone (unavailable: logged out can't
		// tell deleted from private), or a walled page (E10): what the grid
		// gave and the status, counts unset. Partial, so stored content
		// isn't blanked.
		env.Partial = true
		c.Status = &contentv1.Status{Availability: contentv1.Availability_AVAILABILITY_UNAVAILABLE, Reason: p.StatusReason,
			Since: env.ObservedAt}
		if p.StatusReason == "login_wall" {
			c.Status.Availability = contentv1.Availability_AVAILABILITY_RESTRICTED
		}
		if p.IsPinned != nil {
			c.Flags = &contentv1.PostFlags{Pinned: p.IsPinned}
		}
		c.Extras = extras(map[string]any{"ig.shortcode": p.Shortcode, "ig.product_type": p.ProductType})
		seal(env, c)
		return []Out{{Topic: topics.ContentPost, Key: env.Id, Msg: c}}
	}
	c.Text = Text(p.Caption)
	c.Hashtags = p.Hashtags
	for _, m := range p.Mentions {
		c.Mentions = append(c.Mentions, accountRef(0, m))
	}
	var positions []any
	for _, t := range p.Usertags {
		c.Tagged = append(c.Tagged, accountRef(t.UserPk, t.Username))
		pos := map[string]any{"user": t.Username}
		if t.X != nil {
			pos["x"] = *t.X
		}
		if t.Y != nil {
			pos["y"] = *t.Y
		}
		if t.ChildMediaPk != 0 {
			pos["child"] = pk(t.ChildMediaPk)
		}
		positions = append(positions, pos)
	}
	for _, co := range p.Coauthors {
		if co.UserPk != p.OwnerPk {
			c.Coauthors = append(c.Coauthors, accountRef(co.UserPk, co.Username))
		}
	}
	if l := p.Location; l != nil {
		c.Location = &contentv1.Location{Name: l.Name, Latitude: l.Lat, Longitude: l.Lng}
		if l.Pk != 0 {
			c.Location.PlatformLocationId = pk(l.Pk)
		}
	}
	if m := p.Music; m != nil {
		// audio_id is a contract ID, "ig:audio:<native id>", when the
		// audio has one.
		c.Audio = &contentv1.AudioRef{Title: m.Title, Artist: m.Artist, Original: proto.Bool(m.OriginalAudio)}
		if m.AudioId != "" {
			c.Audio.AudioId = ID(KindAudio, m.AudioId)
		}
	}
	if typ != contentv1.PostType_POST_TYPE_POST {
		v := &contentv1.VideoInfo{HasAudio: p.HasAudio}
		if p.VideoDurationMs > 0 {
			v.DurationMs = proto.Int64(p.VideoDurationMs)
		}
		if p.Width > 0 && p.Height > 0 {
			v.Width, v.Height = proto.Int32(p.Width), proto.Int32(p.Height)
		}
		c.Video = v
	}
	// ai_generated only with Instagram's label: no label is unknown, not
	// "not AI" (REQ-019 item 3).
	if p.AiLabel != "" {
		c.Labels = &contentv1.ContentLabels{AiGenerated: proto.Bool(true), AiLabelText: p.AiLabel}
	}
	f := &contentv1.PostFlags{Pinned: p.IsPinned}
	if p.CommentingDisabled {
		f.CommentsDisabled = proto.Bool(true)
	}
	if p.CaptionEdited {
		f.Edited = proto.Bool(true)
	}
	if f.Pinned != nil || f.CommentsDisabled != nil || f.Edited != nil {
		c.Flags = f
	}
	c.PlatformTopics = p.Topics
	c.Counts = postCounts(p.LikeCount, p.CommentCount, p.PlayCount, p.CountsHidden)
	c.Status = &contentv1.Status{Availability: contentv1.Availability_AVAILABILITY_VISIBLE}
	// Replies aren't served logged out (README's mapping table).
	c.Coverage = &contentv1.ItemCoverage{RepliesAvailable: proto.Bool(false)}
	c.Extras = extras(map[string]any{"ig.shortcode": p.Shortcode, "ig.product_type": p.ProductType,
		"ig.media_type": float64(p.MediaType), "ig.usertag_positions": positions,
		"ig.discovered_via": discoveredVia(p.DiscoveredVia)})
	seal(env, c)
	return append([]Out{{Topic: topics.ContentPost, Key: env.Id, Msg: c}}, audio(p.Obs, p.Music)...)
}

func discoveredVia(d igv1.DiscoveredVia) string {
	s := strings.ToLower(strings.TrimPrefix(d.String(), "DISCOVERED_VIA_"))
	if s == "unspecified" {
		return ""
	}
	return s
}

func postMetrics(m *igv1.PostMetrics) []Out {
	native := pk(m.MediaPk)
	env := envelope(m.Obs, KindMetrics, "post", native)
	if m.CapturedAt != nil {
		env.ObservedAt = m.CapturedAt
		env.Version = m.CapturedAt.AsTime().UnixMicro()
	}
	counts := postCounts(m.LikeCount, m.CommentCount, m.PlayCount, m.CountsHidden)
	if counts == nil {
		return nil
	}
	c := &contentv1.Metrics{Envelope: env, TargetKind: "post", ParentId: ID("account", pk(m.OwnerPk)), Counts: counts}
	seal(env, c)
	return []Out{{Topic: topics.ContentMetrics, Key: env.Id, Msg: c}}
}

func comment(c *igv1.Comment) []Out {
	if c.Redacted {
		// An erased author's comment: igs keeps no text, and publishes none.
		return nil
	}
	native := pk(c.CommentPk)
	postID := ID("post", pk(c.MediaPk))
	env := envelope(c.Obs, KindComment, "comment", native)
	out := &contentv1.Comment{Envelope: env, PostId: postID, AccountId: ID("account", pk(c.OwnerPk))}
	if c.ParentCommentPk != nil {
		// Logged out, Instagram serves one level: parent and root are the
		// same comment.
		parent := ID("comment", pk(*c.ParentCommentPk))
		out.ParentCommentId, out.RootCommentId, out.Depth = parent, parent, 1
	}
	if c.Status == igv1.CommentStatus_COMMENT_STATUS_DELETED {
		// Absent from a complete re-read: a partial event with the status.
		// Storage keeps the text it has (README §4).
		env.Partial = true
		out.Status = &contentv1.Status{Availability: contentv1.Availability_AVAILABILITY_DELETED, Since: env.ObservedAt}
		seal(env, out)
		return []Out{{Topic: topics.ContentComment, Key: postID, Msg: out}}
	}
	out.Text = Text(c.Text)
	out.PublishedAt = exactTime(c.CreatedAt)
	out.Status = &contentv1.Status{Availability: contentv1.Availability_AVAILABILITY_VISIBLE}
	if a := c.Author; a != nil {
		ref := accountRef(a.UserPk, a.Username)
		ref.PictureUrl = a.ProfilePicUrl
		if ref.Id == "" && ref.Handle == "" {
			// Pseudonymized: no account is named.
			out.Author = &contentv1.Author{Hidden: true}
		} else {
			ref.Verified = proto.Bool(a.IsVerified)
			out.Author = &contentv1.Author{Account: ref}
		}
	}
	out.Flags = &contentv1.CommentFlags{ByPostOwner: proto.Bool(c.IsOp)}
	seal(env, out)
	res := []Out{{Topic: topics.ContentComment, Key: postID, Msg: out}}
	if c.LikeCount != nil || c.ChildCommentCount != nil {
		menv := envelope(c.Obs, KindMetrics, "comment", native)
		mc := &contentv1.Metrics{Envelope: menv, TargetKind: "comment", ParentId: postID,
			Counts: &contentv1.Engagement{Likes: exact(c.LikeCount), Replies: exact(c.ChildCommentCount)}}
		seal(menv, mc)
		// Keyed by the comment's post, so a post's counts stay in order.
		res = append(res, Out{Topic: topics.ContentMetrics, Key: postID, Msg: mc})
	}
	return res
}
