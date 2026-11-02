package parse

import (
	"crypto/sha1" //nolint:gosec // UUIDv5 is defined over SHA-1
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/common/v1"
	igv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/ig/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/walk"
)

// mediaNamespace is igs's UUIDv5 namespace for media ids (ADR 0005 §1):
// fbs uses another, so the two never collide.
var mediaNamespace = uuid.NewSHA1(uuid.NameSpaceURL, []byte("https://securebrand/igs/media"))

// MediaID is the stable id of one rendition of an Instagram media item.
func MediaID(pk, variant string) string {
	return uuid.NewHash(sha1.New(), mediaNamespace, []byte(pk+"|"+variant), 5).String() //nolint:gosec // UUIDv5
}

// pkEpoch is Instagram's pk epoch: a media pk's upper bits are milliseconds
// since it (U10, confirmed on the probe's posts within a minute of
// taken_at).
const pkEpoch = 1314220021721

// TakenAtFromPK returns the creation time a media pk encodes, or the zero
// time for an id that is not a media pk.
func TakenAtFromPK(pk string) time.Time {
	n, err := strconv.ParseUint(strings.SplitN(pk, "_", 2)[0], 10, 64)
	if err != nil || n == 0 {
		return time.Time{}
	}
	ms := int64(n>>23) + pkEpoch
	t := time.UnixMilli(ms).UTC()
	if t.Year() < 2010 || t.After(time.Now().Add(48*time.Hour)) {
		return time.Time{}
	}
	return t
}

// MediaCtx is what a media row needs from its owner.
type MediaCtx struct {
	UserPK        int64
	MediaPK       int64
	ParentMediaPK *int64
	Position      int32
	CommentPK     *int64
	OwnerType     string // post | comment | profile | highlight
	PostTakenAt   *timestamppb.Timestamp
	AltText       string
}

// Renditions maps one media object's image candidates and video versions
// to media rows (ADR 0005 §7): the largest image is "original", the
// smallest "thumbnail"; videos are "sd" and "hd" by size. A video's still
// image is its thumbnail.
func Renditions(m gjson.Result, mc MediaCtx) []*igv1.Media {
	pk := walk.First(m, "pk", "id").String()
	if i := strings.IndexByte(pk, '_'); i > 0 {
		pk = pk[:i]
	}
	if pk == "" {
		return nil
	}
	isVideo := walk.Int64(m.Get("media_type")) == 2 || m.Get("video_versions").IsArray()
	var out []*igv1.Media
	add := func(kind commonv1.MediaKind, variant string, c gjson.Result) {
		url := c.Get("url").String()
		if url == "" {
			return
		}
		out = append(out, &igv1.Media{
			MediaId: MediaID(pk, variant), SourceMediaId: pk, Kind: kind, Variant: variant, OwnerType: mc.OwnerType,
			UserPk: mc.UserPK, MediaPk: mc.MediaPK, ParentMediaPk: mc.ParentMediaPK, Position: mc.Position, CommentPk: mc.CommentPK,
			SourceUrl: url, SourceExpiresAt: walk.URLExpiry(url), Width: int32(c.Get("width").Int()), Height: int32(c.Get("height").Int()),
			AltText: mc.AltText, PostTakenAt: mc.PostTakenAt,
		})
	}
	cands := m.Get("image_versions2.candidates").Array()
	if len(cands) == 0 {
		if u := walk.First(m, "display_uri", "display_url", "thumbnail_src").String(); u != "" {
			cands = []gjson.Result{gjson.Parse(`{"url":` + strconvQuote(u) + `}`)}
		}
	}
	if len(cands) > 0 {
		big, small := cands[0], cands[0]
		for _, c := range cands[1:] {
			if c.Get("width").Int() > big.Get("width").Int() {
				big = c
			}
			if c.Get("width").Int() < small.Get("width").Int() {
				small = c
			}
		}
		if isVideo {
			add(commonv1.MediaKind_MEDIA_KIND_THUMBNAIL, "thumbnail", big)
		} else {
			add(commonv1.MediaKind_MEDIA_KIND_PHOTO, "original", big)
			if small.Get("url").String() != big.Get("url").String() {
				add(commonv1.MediaKind_MEDIA_KIND_THUMBNAIL, "thumbnail", small)
			}
		}
	}
	if vids := m.Get("video_versions").Array(); len(vids) > 0 {
		kind := commonv1.MediaKind_MEDIA_KIND_VIDEO
		if m.Get("product_type").String() == "clips" {
			kind = commonv1.MediaKind_MEDIA_KIND_REEL
		}
		hd, sd := vids[0], vids[0]
		for _, v := range vids[1:] {
			if v.Get("width").Int() > hd.Get("width").Int() {
				hd = v
			}
			if v.Get("width").Int() < sd.Get("width").Int() {
				sd = v
			}
		}
		start := len(out)
		add(kind, "hd", hd)
		if sd.Get("url").String() != hd.Get("url").String() {
			add(kind, "sd", sd)
		}
		if d := durationMS(m); d > 0 {
			for _, x := range out[start:] {
				x.DurationMs = d
			}
		}
	}
	return out
}

func strconvQuote(s string) string { return strconv.Quote(s) }
