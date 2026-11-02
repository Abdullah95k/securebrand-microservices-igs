package parse

import (
	"strings"

	"github.com/tidwall/gjson"

	igv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/ig/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/walk"
)

// HighlightItems parses a PolarisStoriesV3HighlightsLoggedOutPageQuery
// answer: each highlight reel's items, as Instagram's v1 story items (pk,
// taken_at, media_type, image and video versions). The probe's answers
// carried no items yet, so the shape is the recordings' root field with
// the standard item fields; a fixture pins it.
func HighlightItems(parts []gjson.Result, userPK int64) ([]*igv1.Highlight, []*igv1.Media, walk.Report) {
	var out []*igv1.Highlight
	var media []*igv1.Media
	var rep walk.Report
	found := false
	for _, p := range parts {
		for _, conn := range walk.FindAll(p, "xdt_api__v1__feed__reels_media__connection") {
			found = true
			for _, e := range conn.Get("edges").Array() {
				n := e.Get("node")
				id := walk.Int64(gjson.Parse(`"` + strings.TrimPrefix(walk.First(n, "id", "reel_id").String(), "highlight:") + `"`))
				if id == 0 {
					rep.AddMissing("highlight.id")
					continue
				}
				h := &igv1.Highlight{HighlightId: id, UserPk: userPK, Title: n.Get("title").String()}
				for _, it := range n.Get("items").Array() {
					pkStr := strings.SplitN(walk.First(it, "pk", "id").String(), "_", 2)[0]
					pk := walk.Int64(gjson.Parse(`"` + pkStr + `"`))
					if pk == 0 {
						continue
					}
					ms := Renditions(it, MediaCtx{UserPK: userPK, MediaPK: pk, OwnerType: "highlight"})
					item := &igv1.HighlightItem{ItemPk: pk, TakenAt: walk.Unix(it.Get("taken_at")), MediaType: int32(walk.Int64(it.Get("media_type")))}
					for _, m := range ms {
						item.MediaIds = append(item.MediaIds, m.MediaId)
					}
					h.Items = append(h.Items, item)
					media = append(media, ms...)
				}
				h.ItemCount = int32(len(h.Items))
				out = append(out, h)
			}
		}
	}
	if !found {
		rep.AddMissing("reels_media_connection")
	}
	return out, media, rep
}
