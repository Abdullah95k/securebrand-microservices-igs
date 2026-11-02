package parse

import (
	"strings"

	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/htmlx"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/walk"
)

// Reel is one reel in a profile's reels tab: the only logged-out source of
// its views (HAR findings §8).
type Reel struct {
	MediaPK      int64
	Shortcode    string
	ProductType  string
	PlayCount    *int64
	LikeCount    *int64
	CommentCount *int64
	// CountsHidden: the owner hid the likes; Instagram then answers a like
	// count of 3 (or 0 to 2), so LikeCount is left nil.
	CountsHidden bool
	// Pinned to the reels tab.
	Pinned       bool
	ThumbnailURL string
	TakenAt      *timestamppb.Timestamp
}

// ReelsResult is one reels tab page.
type ReelsResult struct {
	Reels    []Reel
	PageInfo walk.PageInfo
}

var reelsConnections = []string{"polaris_clips_connection", "xdt_api__v1__clips__user__connection_v2", "xdt_api__v1__clips__user__connection"}

// Reels parses a PolarisLoggedOutDesktopWWWProfileReelsTabContentQuery
// answer.
func Reels(parts []gjson.Result) (ReelsResult, walk.Report) {
	var res ReelsResult
	var rep walk.Report
	seen := map[string]bool{}
	found := false
	for _, p := range parts {
		for _, key := range reelsConnections {
			for _, conn := range walk.FindAll(p, key) {
				found = true
				for _, e := range conn.Get("edges").Array() {
					n := e.Get("node")
					if n.Get("media").IsObject() {
						n = n.Get("media")
					}
					r, ok := reel(n)
					if !ok {
						rep.AddMissing("reel.code")
						continue
					}
					if !seen[r.Shortcode] {
						seen[r.Shortcode] = true
						res.Reels = append(res.Reels, r)
					}
				}
			}
		}
	}
	if !found {
		rep.AddMissing("clips_connection")
	}
	res.PageInfo = walk.FindPageInfo(parts, reelsConnections)
	return res, rep
}

// ReelsPage parses the reels a /<username>/reels/ page embeds (the first
// 12), for when the query will not start without a cursor.
func ReelsPage(html []byte) (ReelsResult, walk.Report) {
	return Reels(htmlx.JSONBlobs(html))
}

func reel(n gjson.Result) (Reel, bool) {
	code := n.Get("code").String()
	pkStr := strings.SplitN(walk.First(n, "pk", "id").String(), "_", 2)[0]
	if code == "" || pkStr == "" {
		return Reel{}, false
	}
	count := func(path string) *int64 {
		if v := n.Get(path); v.Type == gjson.Number {
			x := v.Int()
			return &x
		}
		return nil
	}
	r := Reel{
		MediaPK: walk.Int64(gjson.Parse(`"` + pkStr + `"`)), Shortcode: code, ProductType: n.Get("product_type").String(),
		PlayCount: count("play_count"), LikeCount: count("like_count"), CommentCount: count("comment_count"),
		CountsHidden: n.Get("like_and_view_counts_disabled").Bool(), Pinned: len(n.Get("clips_tab_pinned_user_ids").Array()) > 0,
		ThumbnailURL: walk.First(n, "display_uri", "image_versions2.candidates.0.url").String(), TakenAt: pkTime(pkStr),
	}
	if r.PlayCount == nil {
		r.PlayCount = count("ig_play_count")
	}
	if r.CountsHidden {
		r.LikeCount = nil
	}
	return r, true
}
