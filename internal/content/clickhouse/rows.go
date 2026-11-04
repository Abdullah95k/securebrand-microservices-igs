package clickhouse

import (
	"encoding/hex"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	contentv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/content/v1"
)

// Table names and their columns, in insert order.
var columns = map[string][]string{
	"posts": {"platform", "post_id", "account_id", "kind", "title", "text", "description", "language", "published_at", "url",
		"views", "likes", "shares", "comments", "media_count", "coauthor_ids", "quoted_post_id", "extras", "is_deleted", "version",
		"observed_at", "collector", "collector_version", "raw_ref", "event_id"},
	"comments": {"platform", "comment_id", "post_id", "parent_comment_id", "account_id", "author_id", "author_key", "author_name",
		"text", "language", "published_at", "likes", "replies", "extras", "is_deleted", "version", "observed_at", "collector",
		"collector_version", "raw_ref", "event_id"},
	"authors": {"platform", "author_ref", "author_id", "author_key", "name", "profile_url", "picture_url", "verified", "version",
		"observed_at"},
	"post_metrics":      {"platform", "post_id", "account_id", "observed_at", "views", "likes", "shares", "comments", "reactions", "counts_hidden", "raw_ref"},
	"comment_metrics":   {"platform", "comment_id", "post_id", "observed_at", "likes", "replies", "raw_ref"},
	"account_snapshots": {"platform", "account_id", "observed_at", "followers", "following", "post_count", "raw_ref"},
	"accounts": {"platform", "account_id", "handle", "name", "kind", "verified", "category", "country_signals", "bio", "website",
		"link_urls", "link_titles", "picture_url", "followers", "following", "post_count", "is_private", "is_business", "extras",
		"is_deleted", "version", "observed_at", "collector", "collector_version", "raw_ref", "event_id"},
	"media": {"platform", "media_id", "owner_type", "owner_id", "type", "url", "url_expires_at", "width", "height", "duration_ms",
		"alt_text", "position", "storage_key", "analysis_status", "is_deleted", "version", "observed_at", "raw_ref"},
}

// Rows are rows per table.
type Rows map[string][][]any

func (r Rows) add(table string, row ...any) { r[table] = append(r[table], row) }

func b8(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}

func at(t *timestamppb.Timestamp, or time.Time) time.Time {
	if t == nil || t.AsTime().IsZero() {
		return or.UTC()
	}
	return t.AsTime().UTC()
}

func observed(e *contentv1.Envelope) time.Time {
	if e.GetObservedAt() == nil {
		return time.Unix(0, 0).UTC()
	}
	return e.ObservedAt.AsTime().UTC()
}

// version is the envelope's version, or its observation time (micros).
func version(e *contentv1.Envelope) uint64 {
	if v := e.GetVersion(); v > 0 {
		return uint64(v)
	}
	return uint64(observed(e).UnixMicro())
}

func strs(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// Add maps one content event to rows.
func (r Rows) Add(m proto.Message) {
	switch v := m.(type) {
	case *contentv1.Post:
		e := v.GetEnvelope()
		r.add("posts", e.GetPlatform(), e.GetNativeId(), v.AccountId, v.Kind, v.Title, v.Text, v.Description, v.Language,
			at(e.GetPublishedAt(), observed(e)), v.Url, v.Views, v.Likes, v.Shares, v.Comments, uint32(v.MediaCount),
			strs(v.CoauthorIds), v.QuotedPostId, v.ExtrasJson, b8(v.IsDeleted), version(e), observed(e), e.GetCollector(),
			e.GetCollectorVersion(), e.GetRawRef(), e.GetEventId())
	case *contentv1.Comment:
		e := v.GetEnvelope()
		var authorID, authorKey, authorName string
		if a := v.Author; a != nil {
			authorID, authorKey, authorName = a.AuthorId, hex.EncodeToString(a.AuthorKey), a.Name
			ref := authorID
			if ref == "" {
				ref = authorKey
			}
			if ref != "" {
				r.add("authors", e.GetPlatform(), ref, authorID, authorKey, a.Name, a.ProfileUrl, a.PictureUrl, b8(a.Verified),
					version(e), observed(e))
			}
		}
		r.add("comments", e.GetPlatform(), e.GetNativeId(), v.PostId, v.ParentCommentId, v.AccountId, authorID, authorKey, authorName,
			v.Text, v.Language, at(e.GetPublishedAt(), observed(e)), v.Likes, v.Replies, v.ExtrasJson, b8(v.IsDeleted), version(e),
			observed(e), e.GetCollector(), e.GetCollectorVersion(), e.GetRawRef(), e.GetEventId())
	case *contentv1.Account:
		e := v.GetEnvelope()
		urls, titles := []string{}, []string{}
		for _, l := range v.Links {
			urls, titles = append(urls, l.Url), append(titles, l.Title)
		}
		r.add("accounts", e.GetPlatform(), e.GetNativeId(), v.Handle, v.Name, v.Kind, b8(v.Verified), v.Category, strs(v.CountrySignals),
			v.Bio, v.Website, urls, titles, v.PictureUrl, v.Followers, v.Following, v.PostCount, b8(v.IsPrivate), b8(v.IsBusiness),
			v.ExtrasJson, b8(v.IsDeleted), version(e), observed(e), e.GetCollector(), e.GetCollectorVersion(), e.GetRawRef(),
			e.GetEventId())
	case *contentv1.Metrics:
		e := v.GetEnvelope()
		switch v.Target {
		case "post":
			reactions := v.Reactions
			if reactions == nil {
				reactions = map[string]int64{}
			}
			r.add("post_metrics", e.GetPlatform(), e.GetNativeId(), v.ParentId, observed(e), v.Views, v.Likes, v.Shares, v.Comments,
				reactions, b8(v.CountsHidden), e.GetRawRef())
		case "comment":
			r.add("comment_metrics", e.GetPlatform(), e.GetNativeId(), v.ParentId, observed(e), v.Likes, v.Replies, e.GetRawRef())
		case "account":
			r.add("account_snapshots", e.GetPlatform(), e.GetNativeId(), observed(e), v.Followers, v.Following, v.PostCount, e.GetRawRef())
		}
	case *contentv1.Media:
		e := v.GetEnvelope()
		var expires *time.Time
		if v.UrlExpiresAt != nil {
			t := v.UrlExpiresAt.AsTime().UTC()
			expires = &t
		}
		r.add("media", e.GetPlatform(), e.GetNativeId(), v.OwnerType, v.OwnerId, v.Type, v.Url, expires, v.Width, v.Height,
			v.DurationMs, v.AltText, v.Position, "", "", uint8(0), version(e), observed(e), e.GetRawRef())
	}
}
