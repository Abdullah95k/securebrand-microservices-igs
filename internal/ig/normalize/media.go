package normalize

import (
	"strings"

	"google.golang.org/protobuf/types/known/timestamppb"

	contentv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/content/v1"

	commonv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/common/v1"
	igv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/ig/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/topics"
)

// MediaID is a media item's contract ID: igs's own UUIDv5 of
// <pk>|<variant>, stable across reads, as the contract asks.
func MediaID(uuid string) string { return ID(KindMedia, uuid) }

// NativeMediaID is the UUID inside an igs media ID, and false for another
// platform's or an unknown ID (content.media.processed.v1 is shared).
func NativeMediaID(id string) (string, bool) {
	uuid, ok := strings.CutPrefix(id, Platform+":"+KindMedia+":")
	return uuid, ok && uuid != ""
}

// MediaTypeRole maps igs's media kind to the contract's type and role, and
// the platform's own finer kind. ok is false for kinds published with a
// later slice (highlight items: REQ-019 slice 3).
func MediaTypeRole(k commonv1.MediaKind) (typ contentv1.MediaType, role contentv1.MediaRole, platformKind string, ok bool) {
	platformKind = strings.ToLower(strings.TrimPrefix(k.String(), "MEDIA_KIND_"))
	switch k {
	case commonv1.MediaKind_MEDIA_KIND_PHOTO:
		return contentv1.MediaType_MEDIA_TYPE_IMAGE, contentv1.MediaRole_MEDIA_ROLE_CONTENT, platformKind, true
	case commonv1.MediaKind_MEDIA_KIND_VIDEO, commonv1.MediaKind_MEDIA_KIND_REEL:
		return contentv1.MediaType_MEDIA_TYPE_VIDEO, contentv1.MediaRole_MEDIA_ROLE_CONTENT, platformKind, true
	case commonv1.MediaKind_MEDIA_KIND_THUMBNAIL:
		return contentv1.MediaType_MEDIA_TYPE_IMAGE, contentv1.MediaRole_MEDIA_ROLE_THUMBNAIL, platformKind, true
	case commonv1.MediaKind_MEDIA_KIND_COVER:
		return contentv1.MediaType_MEDIA_TYPE_IMAGE, contentv1.MediaRole_MEDIA_ROLE_COVER, platformKind, true
	case commonv1.MediaKind_MEDIA_KIND_PROFILE_PIC:
		return contentv1.MediaType_MEDIA_TYPE_IMAGE, contentv1.MediaRole_MEDIA_ROLE_PROFILE_PICTURE, platformKind, true
	case commonv1.MediaKind_MEDIA_KIND_COMMENT_PHOTO:
		return contentv1.MediaType_MEDIA_TYPE_IMAGE, contentv1.MediaRole_MEDIA_ROLE_ATTACHMENT, platformKind, true
	case commonv1.MediaKind_MEDIA_KIND_COMMENT_GIF:
		return contentv1.MediaType_MEDIA_TYPE_GIF, contentv1.MediaRole_MEDIA_ROLE_ATTACHMENT, platformKind, true
	case commonv1.MediaKind_MEDIA_KIND_COMMENT_VIDEO:
		return contentv1.MediaType_MEDIA_TYPE_VIDEO, contentv1.MediaRole_MEDIA_ROLE_ATTACHMENT, platformKind, true
	case commonv1.MediaKind_MEDIA_KIND_STICKER:
		return contentv1.MediaType_MEDIA_TYPE_STICKER, contentv1.MediaRole_MEDIA_ROLE_ATTACHMENT, platformKind, true
	}
	return 0, 0, platformKind, false
}

// MediaOwner is the contract ID and kind of what a media item belongs to:
// the post (a carousel child's parent), the comment or the account.
// Highlight media waits for REQ-019 slice 3.
func MediaOwner(ownerType string, userPk, mediaPk int64, parentPk, commentPk *int64) (id, kind string, ok bool) {
	switch ownerType {
	case "post":
		if parentPk != nil && *parentPk != 0 {
			mediaPk = *parentPk
		}
		if mediaPk != 0 {
			return ID(KindPost, pk(mediaPk)), KindPost, true
		}
	case "comment":
		if commentPk != nil && *commentPk != 0 {
			return ID(KindComment, pk(*commentPk)), KindComment, true
		}
	case "profile":
		if userPk != 0 {
			return ID(KindAccount, pk(userPk)), KindAccount, true
		}
	}
	return "", "", false
}

// media maps ig.media.v1 to content.media.v1, keyed by the media ID.
func media(m *igv1.Media) []Out {
	typ, role, pkind, ok := MediaTypeRole(m.Kind)
	owner, ownerKind, owned := MediaOwner(m.OwnerType, m.UserPk, m.MediaPk, m.ParentMediaPk, m.CommentPk)
	if !ok || !owned || m.MediaId == "" {
		return nil
	}
	if m.ParentMediaPk != nil && *m.ParentMediaPk != 0 {
		pkind = "carousel_child"
	}
	env := envelope(m.Obs, KindMedia, KindMedia, m.MediaId)
	c := &contentv1.Media{Envelope: env, OwnerId: owner, OwnerKind: ownerKind, Type: typ, Role: role, PlatformKind: pkind,
		Variant: m.Variant, Position: m.Position, PlatformMediaId: m.SourceMediaId, SourceUrl: m.SourceUrl,
		SourceExpiresAt: m.SourceExpiresAt, Mime: m.Mime, AltText: m.AltText}
	if m.Width > 0 {
		c.Width = &m.Width
	}
	if m.Height > 0 {
		c.Height = &m.Height
	}
	if m.DurationMs > 0 {
		c.DurationMs = &m.DurationMs
	}
	seal(env, c)
	return []Out{{Topic: topics.ContentMedia, Key: env.Id, Msg: c}}
}

// signedURL clears what changes on every read of the same file (Instagram
// signs its CDN URLs anew), so it doesn't change the content hash.
func signedURL(m *contentv1.Media) {
	m.SourceUrl, m.SourceExpiresAt = "", (*timestamppb.Timestamp)(nil)
}
