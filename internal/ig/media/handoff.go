package media

import (
	"context"
	"encoding/hex"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"

	contentv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/content/v1"
	mediav1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/media/v1"

	commonv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/common/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/normalize"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/buildinfo"
)

// The contract's media lifetime (README §8, owner R5): every stored file
// has a deadline at least a day after it was stored, three days unless the
// owner sets another.
const (
	MinLifetime     = 24 * time.Hour
	DefaultLifetime = 72 * time.Hour
)

// Lifetime is how long a stored file may wait for its acknowledgements:
// media.ttl, or the contract's 72 hours when it is 0, and never under 24.
func Lifetime(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return DefaultLifetime
	}
	return max(ttl, MinLifetime)
}

// readyEvent is the content.media.ready.v1 announcement of a stored file,
// from its media row. attempt counts announcements from 1.
func readyEvent(ctx context.Context, db *pgxpool.Pool, mediaID string, attempt int32, consumers, jobIDs []string) (*mediav1.MediaReady, error) {
	var kind, variant, ownerType string
	var user, post, parent, comment, size, dur *int64
	var w, h *int32
	var sha []byte
	var bucket, key, ct, alt *string
	var storedAt, deleteAfter *time.Time
	err := db.QueryRow(ctx, `SELECT kind, variant, owner_type, user_pk, media_pk, parent_media_pk, comment_pk, width, height,
		duration_ms, sha256, s3_bucket, s3_key, size_bytes, mime, alt_text, stored_at, delete_after FROM media WHERE media_id = $1`, mediaID).
		Scan(&kind, &variant, &ownerType, &user, &post, &parent, &comment, &w, &h, &dur, &sha, &bucket, &key, &size, &ct, &alt, &storedAt, &deleteAfter)
	if err != nil {
		return nil, err
	}
	typ, role, _, _ := normalize.MediaTypeRole(commonv1.MediaKind(commonv1.MediaKind_value["MEDIA_KIND_"+strings.ToUpper(kind)]))
	owner, ownerKind, _ := normalize.MediaOwner(ownerType, val(user), val(post), parent, comment)
	r := &mediav1.MediaReady{
		EventId: normalize.MediaID(mediaID) + ":ready:" + strconv.Itoa(int(attempt)), Platform: normalize.Platform,
		MediaId: normalize.MediaID(mediaID), OwnerId: owner, OwnerKind: ownerKind, Type: typ, Role: role, Variant: variant,
		Object: &mediav1.StorageObject{Bucket: str(bucket), Key: str(key), SizeBytes: val(size), Sha256: hex.EncodeToString(sha),
			ContentType: str(ct)},
		Width: w, Height: h, DurationMs: dur, AltText: str(alt), RequiredConsumers: consumers, Attempt: attempt,
		Provenance: &contentv1.Provenance{Collector: "igs", CollectorVersion: buildinfo.Version(), JobIds: jobIDs,
			Acquisition: contentv1.Acquisition_ACQUISITION_SCRAPED_LOGGED_OUT},
	}
	if v := val(user); v != 0 {
		r.AccountId = normalize.ID(normalize.KindAccount, strconv.FormatInt(v, 10))
	}
	if storedAt != nil {
		r.StoredAt = timestamppb.New(*storedAt)
	}
	if deleteAfter != nil {
		r.DeleteAfter = timestamppb.New(*deleteAfter)
	}
	return r, nil
}

// deletedEvent records a file's removal on content.media.deleted.v1.
func deletedEvent(mediaID string, reason mediav1.DeleteReason, at time.Time) *mediav1.MediaDeleted {
	name := strings.ToLower(strings.TrimPrefix(reason.String(), "DELETE_REASON_"))
	return &mediav1.MediaDeleted{EventId: normalize.MediaID(mediaID) + ":deleted:" + name, MediaId: normalize.MediaID(mediaID),
		Reason: reason, DeletedAt: timestamppb.New(at), Platform: normalize.Platform}
}
