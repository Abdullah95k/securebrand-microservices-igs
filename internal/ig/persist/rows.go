package persist

import (
	"encoding/json"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/common/v1"
	igv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/ig/v1"
)

func observed(o *commonv1.Observation) time.Time {
	if o == nil || o.ObservedAt == nil {
		return time.Now().UTC()
	}
	return o.ObservedAt.AsTime()
}

func hashOf(o *commonv1.Observation) []byte {
	if o == nil || o.ContentHash == nil {
		return []byte{}
	}
	return o.ContentHash
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func nilIfZero(n int64) *int64 {
	if n == 0 {
		return nil
	}
	return &n
}

func ts(t *timestamppb.Timestamp) *time.Time {
	if t == nil {
		return nil
	}
	v := t.AsTime()
	return &v
}

// jsonOf marshals v, or returns nil for an empty value.
func jsonOf(v any) []byte {
	if v == nil {
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil || string(b) == "null" || string(b) == "[]" || string(b) == "{}" {
		return nil
	}
	return b
}

// protoJSON marshals a message with proto field names, or nil.
func protoJSON(m proto.Message) []byte {
	if m == nil || !m.ProtoReflect().IsValid() {
		return nil
	}
	b, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(m)
	if err != nil || string(b) == "{}" {
		return nil
	}
	return b
}

// protoJSONList marshals a list of messages as a JSON array, or nil.
func protoJSONList[T proto.Message](xs []T) []byte {
	if len(xs) == 0 {
		return nil
	}
	parts := make([]json.RawMessage, 0, len(xs))
	for _, x := range xs {
		b, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(x)
		if err != nil {
			return nil
		}
		parts = append(parts, b)
	}
	return jsonOf(parts)
}

func enumName(s, prefix string) string { return strings.ToLower(strings.TrimPrefix(s, prefix)) }

func profileStatus(s igv1.ProfileStatus) string {
	switch s {
	case igv1.ProfileStatus_PROFILE_STATUS_PRIVATE:
		return "private"
	case igv1.ProfileStatus_PROFILE_STATUS_UNAVAILABLE:
		return "unavailable"
	case igv1.ProfileStatus_PROFILE_STATUS_SUPPRESSED:
		return "suppressed"
	}
	return "active"
}

func discoveredVia(d igv1.DiscoveredVia) string {
	if d == igv1.DiscoveredVia_DISCOVERED_VIA_UNSPECIFIED {
		return "grid"
	}
	return enumName(d.String(), "DISCOVERED_VIA_")
}

var postCols = []string{"taken_at", "media_pk", "owner_pk", "shortcode", "media_type", "product_type", "caption", "caption_edited",
	"hashtags", "mentions", "accessibility_caption", "location", "usertags", "coauthors", "music", "topics", "ai_label",
	"like_count", "counts_hidden", "comment_count", "commenting_disabled", "play_count", "is_pinned", "child_count",
	"video_duration_ms", "width", "height", "has_audio", "media_ids", "discovered_via", "permalink", "content_hash",
	"status", "first_seen_at", "last_seen_at"}

func postRow(p *igv1.Post) []any {
	seen := observed(p.Obs)
	var children *int32
	if n := int32(len(p.Children)); n > 0 {
		children = &n
	}
	var dur *int64
	if p.VideoDurationMs > 0 {
		dur = &p.VideoDurationMs
	}
	var w, h *int32
	if p.Width > 0 {
		w = &p.Width
	}
	if p.Height > 0 {
		h = &p.Height
	}
	return []any{p.TakenAt.AsTime(), p.MediaPk, p.OwnerPk, p.Shortcode, int16(p.MediaType), nilIfEmpty(p.ProductType),
		nilIfEmpty(p.Caption), p.CaptionEdited, p.Hashtags, p.Mentions, nilIfEmpty(p.AccessibilityCaption),
		protoJSON(p.Location), protoJSONList(p.Usertags), protoJSONList(p.Coauthors), protoJSON(p.Music), p.Topics,
		nilIfEmpty(p.AiLabel), p.LikeCount, p.CountsHidden, p.CommentCount, p.CommentingDisabled, p.PlayCount, p.IsPinned,
		children, dur, w, h, p.HasAudio, p.MediaIds, discoveredVia(p.DiscoveredVia), nilIfEmpty(p.Permalink), hashOf(p.Obs),
		"visible", seen, seen}
}

var commentCols = []string{"taken_at", "media_pk", "comment_pk", "owner_pk", "parent_comment_pk", "depth", "author_key",
	"author_pk", "text", "created_at", "like_count", "is_op", "child_comment_count", "status", "redacted", "content_hash",
	"first_seen_at", "last_seen_at"}

func commentRow(c *igv1.Comment) []any {
	seen := observed(c.Obs)
	var key []byte
	var authorPK *int64
	if c.Author != nil {
		key = c.Author.AuthorKey
		if !c.Redacted {
			authorPK = nilIfZero(c.Author.UserPk)
		}
	}
	return []any{c.PostTakenAt.AsTime(), c.MediaPk, c.CommentPk, c.OwnerPk, c.ParentCommentPk, int16(c.Depth), key, authorPK,
		nilIfEmpty(c.Text), ts(c.CreatedAt), c.LikeCount, c.IsOp, c.ChildCommentCount, "visible", c.Redacted, hashOf(c.Obs), seen, seen}
}

var mediaCols = []string{"media_id", "source_media_id", "variant", "kind", "owner_type", "user_pk", "media_pk", "parent_media_pk",
	"position", "comment_pk", "source_url", "source_expires_at", "width", "height", "duration_ms", "mime", "alt_text", "discovered_at"}

func mediaRow(m *igv1.Media) []any {
	var w, h *int32
	if m.Width > 0 {
		w = &m.Width
	}
	if m.Height > 0 {
		h = &m.Height
	}
	return []any{m.MediaId, m.SourceMediaId, m.Variant, enumName(m.Kind.String(), "MEDIA_KIND_"), m.OwnerType, nilIfZero(m.UserPk),
		nilIfZero(m.MediaPk), m.ParentMediaPk, m.Position, m.CommentPk, nilIfEmpty(m.SourceUrl), ts(m.SourceExpiresAt), w, h,
		nilIfZero(m.DurationMs), nilIfEmpty(m.Mime), nilIfEmpty(m.AltText), observed(m.Obs)}
}
