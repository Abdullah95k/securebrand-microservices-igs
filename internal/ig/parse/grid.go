package parse

import (
	"strings"

	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/types/known/timestamppb"

	igv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/ig/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/walk"
)

// GridItem is one post as the profile grid shows it: no counts, no exact
// time (the pk encodes it), no renditions; the post page has those.
type GridItem struct {
	MediaPK      int64
	Shortcode    string
	MediaType    igv1.MediaType
	ProductType  string
	Caption      string
	AltText      string
	ThumbnailURL string
	OwnerPK      int64
	OwnerID      string
	// TakenAt is decoded from the pk (U10).
	TakenAt *timestamppb.Timestamp
	Pinned  bool
}

// GridResult is one grid page.
type GridResult struct {
	Items    []GridItem
	PageInfo walk.PageInfo
	// UserID is the owner's id (17841…), for the next page's query.
	UserID string
}

// gridConnections are the connection names a grid answer has carried.
var gridConnections = []string{"polaris_ordered_timeline_connection", "xdt_api__v1__feed__user_timeline_graphql_connection", "edge_owner_to_timeline_media"}

// Grid parses a grid page (PolarisLoggedOutDesktopWWWProfilePostsTabContentQuery
// and its _connection pages). Relay may stream the connection in parts,
// with its edges and page_info after a first part without them, so every
// part is read.
func Grid(parts []gjson.Result) (GridResult, walk.Report) {
	var res GridResult
	var rep walk.Report
	seen := map[string]bool{}
	found := false
	for _, p := range parts {
		for _, key := range gridConnections {
			for _, conn := range walk.FindAll(p, key) {
				found = true
				for _, e := range conn.Get("edges").Array() {
					it, ok := gridItem(e.Get("node"), &rep)
					if !ok || seen[it.Shortcode] {
						continue
					}
					seen[it.Shortcode] = true
					res.Items = append(res.Items, it)
				}
			}
		}
		if res.UserID == "" {
			res.UserID = walk.First(p, "data.xig_user_by_username.id", "data.user.id", "data.node.id").String()
		}
	}
	// A streamed edge part: {"data":{"node":{...}}, "label": "..."} with no
	// connection around it.
	for _, p := range parts {
		if strings.Contains(p.Get("label").String(), "timeline") {
			if n := p.Get("data.node"); n.Get("code").Exists() {
				if it, ok := gridItem(n, &rep); ok && !seen[it.Shortcode] {
					seen[it.Shortcode] = true
					res.Items = append(res.Items, it)
				}
			}
		}
	}
	if !found && len(res.Items) == 0 {
		rep.AddMissing("timeline_connection")
	}
	res.PageInfo = walk.FindPageInfo(parts, gridConnections)
	markPinned(res.Items)
	return res, rep
}

func gridItem(n gjson.Result, rep *walk.Report) (GridItem, bool) {
	code := n.Get("code").String()
	pkStr := strings.SplitN(walk.First(n, "pk", "id").String(), "_", 2)[0]
	pk := walk.Int64(gjson.Parse(`"` + pkStr + `"`))
	if code == "" || pk == 0 {
		rep.AddMissing("node.code")
		return GridItem{}, false
	}
	it := GridItem{
		MediaPK: pk, Shortcode: code, MediaType: igv1.MediaType(walk.Int64(n.Get("media_type"))),
		ProductType: n.Get("product_type").String(), Caption: walk.First(n, "caption.text", "edge_media_to_caption.edges.0.node.text").String(),
		AltText: n.Get("accessibility_caption").String(), ThumbnailURL: walk.First(n, "display_uri", "display_url", "thumbnail_src").String(),
		OwnerPK: walk.Int64(walk.First(n, "user.pk", "owner.pk")), OwnerID: walk.First(n, "user.id", "owner.id").String(),
	}
	if t := TakenAtFromPK(pkStr); !t.IsZero() {
		it.TakenAt = timestamppb.New(t)
	}
	if ta := walk.Unix(n.Get("taken_at")); ta != nil {
		it.TakenAt = ta
	}
	return it, true
}

// markPinned marks items among the first three that are older than a later
// item: the grid shows pinned posts first, and nothing else flags them.
func markPinned(items []GridItem) {
	for i := range min(3, len(items)) {
		if items[i].TakenAt == nil {
			continue
		}
		for _, later := range items[i+1:] {
			if later.TakenAt != nil && later.TakenAt.AsTime().After(items[i].TakenAt.AsTime()) {
				items[i].Pinned = true
				break
			}
		}
	}
}
