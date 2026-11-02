package fakeig

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// AppID is the app id the fake expects in x-ig-app-id.
const AppID = "936619743392459"

// ExitHeader names the exit a request came through, set by a test
// transport (igtest's Exits), for Faults.WallExits.
const ExitHeader = "X-Fakeig-Exit"

// Friendly names of the five operations.
const (
	OpProfile    = "PolarisLoggedOutDesktopWWWProfileRootContentQuery"
	OpGridFirst  = "PolarisLoggedOutDesktopWWWProfilePostsTabContentQuery"
	OpGrid       = "PolarisLoggedOutDesktopWWWProfilePostsTabContentQuery_connection"
	OpComments   = "PolarisLoggedOutDesktopWWWPostCommentsPaginationQuery"
	OpHighlights = "PolarisStoriesV3HighlightsLoggedOutPageQuery"
	OpReels      = "PolarisLoggedOutDesktopWWWProfileReelsTabContentQuery_connection"
)

// GridPageSize and CommentPageSize are the recorded page sizes.
const (
	GridPageSize    = 12
	CommentPageSize = 10
	// ReelsPageSize is the reels tab's page (the owner's recording); its
	// page HTML embeds the first 12.
	ReelsPageSize = 4
)

// Faults model walls and the open questions.
type Faults struct {
	// RateLimitEvery answers every Nth GraphQL request with HTTP 429.
	RateLimitEvery int
	// LoginWallAfter walls an identity after N GraphQL requests: its
	// queries get a login page, its page loads a redirect to
	// /accounts/login/.
	LoginWallAfter int
	// Challenge redirects every page load to /challenge/.
	Challenge bool
	// MaxPageSize caps the page size a query may ask for ("first"), as
	// Instagram may; 0 serves whatever is asked.
	MaxPageSize int
	// NoCursorReels refuses the reels query without a cursor, so the first
	// page must come from the reels page's HTML.
	NoCursorReels bool
	// PageWallFirst redirects the first N page loads to /accounts/login/,
	// whoever asks: exits already flagged for page loads, as some of the
	// owner's ISP exits were (fourth and full probe runs).
	PageWallFirst int
	// WallExits redirects every page load that comes through one of these
	// exits to /accounts/login/: flagged ISP exits (three of the owner's
	// ten in the details run). The exit is the ExitHeader a test transport
	// sets.
	WallExits []string
	// GridWallAfter walls grid pages that start at or past this many posts
	// (U1: the browser stopped at 24).
	GridWallAfter int
	// RequireClientState fails queries sent without __dyn (U3).
	RequireClientState bool
	// CursorBound makes a cursor work only for the identity it was
	// issued to (U5).
	CursorBound bool
	// NoPKGrid fails the grid query sent without a cursor (U9).
	NoPKGrid bool
	// StreamGrid answers grid queries the way Relay streams a connection:
	// the first part holds the connection with no edges and a placeholder
	// page_info (has_next_page false), each edge follows in its own part
	// with a path, and the real page_info comes last in a deferred part. A
	// reader of the first part alone sees 0 posts and no next page.
	StreamGrid bool
}

// Options configure a server.
type Options struct {
	// Docs maps each friendly name to its accepted doc_ids; nil accepts any.
	Docs   map[string][]string
	Faults Faults
}

// Server serves a World.
type Server struct {
	World *World
	opts  Options

	mu     sync.Mutex
	idents map[string]*ident // by lsd
	byCSRF map[string]*ident
	minted uint64
	// pageLoads counts page loads, for PageWallFirst.
	pageLoads int
	gql       atomic.Int64
	// Requests counts GraphQL requests by friendly name.
	Requests sync.Map
}

type ident struct {
	lsd, csrf string
	queries   int
}

// NewServer returns a server for w.
func NewServer(w *World, o Options) *Server {
	return &Server{World: w, opts: o, idents: map[string]*ident{}, byCSRF: map[string]*ident{}}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasPrefix(r.URL.Path, "/accounts/login"):
		page(w, http.StatusOK, `<html><form action="/accounts/login/ajax/" method="post"></form></html>`)
	case strings.HasPrefix(r.URL.Path, "/challenge"):
		page(w, http.StatusOK, `<html><form action="/challenge/action/" method="post"></form></html>`)
	case strings.HasPrefix(r.URL.Path, "/fake-cdn/"):
		// A stand-in for the image CDN: a few bytes with a JPEG header.
		w.Header().Set("Content-Type", "image/jpeg")
		_, _ = w.Write([]byte("\xff\xd8\xff\xe0fake-jpeg" + r.URL.Path))
	case r.Method == http.MethodPost && (r.URL.Path == "/api/graphql" || r.URL.Path == "/graphql/query"):
		s.World.mu.RLock()
		defer s.World.mu.RUnlock()
		s.graphql(w, r)
	case r.Method == http.MethodGet:
		s.World.mu.RLock()
		defer s.World.mu.RUnlock()
		s.page(w, r)
	default:
		http.NotFound(w, r)
	}
}

// page serves a profile or post page: fresh tokens, cookies and embedded
// data, as a plain GET of any public page did in the recordings.
func (s *Server) page(w http.ResponseWriter, r *http.Request) {
	if s.opts.Faults.Challenge {
		http.Redirect(w, r, "/challenge/?next="+r.URL.Path, http.StatusFound)
		return
	}
	s.mu.Lock()
	s.pageLoads++
	early := s.pageLoads <= s.opts.Faults.PageWallFirst
	s.mu.Unlock()
	if early || slices.Contains(s.opts.Faults.WallExits, r.Header.Get(ExitHeader)) {
		http.Redirect(w, r, "/accounts/login/?next="+r.URL.Path, http.StatusFound)
		return
	}
	if c, err := r.Cookie("csrftoken"); err == nil && s.walled(s.identByCSRF(c.Value)) {
		http.Redirect(w, r, "/accounts/login/?next="+r.URL.Path, http.StatusFound)
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	var data any
	switch {
	case len(parts) == 2 && (parts[0] == "p" || parts[0] == "reel"):
		post := s.postByCode(parts[1])
		if post == nil {
			http.NotFound(w, r)
			return
		}
		// The post page embeds the media object and its first comments
		// (the probe's post pages: the media under if_not_gated_logged_out,
		// null for a gated post, the comments beside it).
		holder := map[string]any{"id": "POLARIS_" + str(post.PK), "if_not_gated_logged_out": nil}
		if !post.Gated {
			holder["if_not_gated_logged_out"] = mediaJSON(post, true)
			holder["comments_connection"] = s.commentPage(post, 0, CommentPageSize, "")
		}
		data = map[string]any{"xig_polaris_media": holder}
	case len(parts) == 2 && parts[1] == "reels":
		p := s.World.Profile(parts[0])
		if p == nil {
			http.NotFound(w, r)
			return
		}
		data = map[string]any{"xig_user_by_username": map[string]any{"pk": str(p.PK), "id": p.ID,
			"polaris_clips_connection": s.reelsPage(p, 0, 12, "")}}
	case len(parts) == 1 && parts[0] != "":
		p := s.World.Profile(parts[0])
		if p == nil {
			http.NotFound(w, r)
			return
		}
		if !strings.HasSuffix(r.URL.Path, "/") {
			http.Redirect(w, r, r.URL.Path+"/", http.StatusMovedPermanently)
			return
		}
		data = map[string]any{"xig_user_by_username": map[string]any{"pk": str(p.PK), "id": p.ID, "username": p.Username,
			"polaris_ordered_timeline_connection": s.gridPage(p, 0, GridPageSize, "")}}
	default:
		http.NotFound(w, r)
		return
	}
	id := s.mint()
	for _, c := range []*http.Cookie{{Name: "csrftoken", Value: id.csrf}, {Name: "mid", Value: "fakemid" + id.lsd},
		{Name: "ig_did", Value: "fake-device-" + id.lsd}} {
		c.Path = "/"
		http.SetCookie(w, c)
	}
	blob, _ := json.Marshal(map[string]any{"result": map[string]any{"data": data}})
	page(w, http.StatusOK, fmt.Sprintf(`<html><head><script type="application/json" data-sjs>{"require":[["ScheduledServerJS","handle",null,[{"__bbox":{"define":[`+
		`["LSD",[],{"token":"%s"},323],["InstagramSecurityConfig",[],{"csrf_token":"%s"},1],`+
		`["SiteData",[],{"server_revision":1029000000,"haste_session":"20400.HYP:instagram_web_pkg.2.1...0","hsi":"7560000000000000001",`+
		`"__spin_r":1029000000,"__spin_b":"trunk","__spin_t":1761800000},317],`+
		`["RelayAPIConfigDefaults",[],{"customHeaders":{"X-IG-App-ID":"%s"}},1],["PolarisSiteData",[],{"rollout_hash":"1029000000"},1],`+
		`["SprinkleConfig",[],{"jazoest":"22001"},1]]}}]]],"__comet_req":7}</script></head><body>`+
		`<script type="application/json" data-sjs>%s</script></body></html>`, id.lsd, id.csrf, AppID, blob))
}

func (s *Server) mint() *ident {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.minted++
	id := &ident{lsd: fmt.Sprintf("AVfake%06d", s.minted), csrf: fmt.Sprintf("csrffake%06d", s.minted)}
	s.idents[id.lsd] = id
	s.byCSRF[id.csrf] = id
	return id
}

func (s *Server) identByCSRF(csrf string) *ident {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.byCSRF[csrf]
}

func (s *Server) walled(id *ident) bool {
	if id == nil || s.opts.Faults.LoginWallAfter <= 0 {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return id.queries >= s.opts.Faults.LoginWallAfter
}

func (s *Server) graphql(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	f := r.PostForm
	friendly := f.Get("fb_api_req_friendly_name")
	n, _ := s.Requests.LoadOrStore(friendly, new(atomic.Int64))
	n.(*atomic.Int64).Add(1)
	if k := s.opts.Faults.RateLimitEvery; k > 0 && s.gql.Add(1)%int64(k) == 0 {
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}
	s.mu.Lock()
	id := s.idents[f.Get("lsd")]
	over := false
	if id != nil {
		id.queries++
		over = s.opts.Faults.LoginWallAfter > 0 && id.queries > s.opts.Faults.LoginWallAfter
	}
	s.mu.Unlock()
	switch {
	case id == nil, r.Header.Get("x-fb-lsd") != id.lsd:
		softError(w, "unknown or mismatched lsd")
		return
	case r.Header.Get("x-csrftoken") != id.csrf:
		softError(w, "csrf token missing or wrong")
		return
	case r.Header.Get("x-ig-app-id") != AppID:
		softError(w, "app id missing or wrong")
		return
	case r.Header.Get("x-fb-friendly-name") != friendly:
		softError(w, "friendly name header and form differ")
		return
	case over:
		page(w, http.StatusOK, `<html><form action="/accounts/login/ajax/" method="post"></form></html>`)
		return
	case s.opts.Faults.RequireClientState && f.Get("__dyn") == "":
		softError(w, "client state missing")
		return
	}
	if docs, ok := s.opts.Docs[friendly]; s.opts.Docs != nil && (!ok || !slices.Contains(docs, f.Get("doc_id"))) {
		write(w, map[string]any{"errors": []any{map[string]any{"message": "PersistedQueryNotFound: doc_id " + f.Get("doc_id") + " is not a known persisted query"}}})
		return
	}
	if (friendly == OpHighlights) != (r.URL.Path == "/graphql/query") ||
		(friendly == OpHighlights && r.Header.Get("x-root-field-name") == "") {
		write(w, map[string]any{"errors": []any{map[string]any{"message": "unknown query on this endpoint"}}})
		return
	}
	var vars map[string]any
	dec := json.NewDecoder(strings.NewReader(f.Get("variables")))
	dec.UseNumber() // pks pass 2^53
	if err := dec.Decode(&vars); err != nil {
		softError(w, "bad variables")
		return
	}
	switch friendly {
	case OpProfile:
		p := s.World.Profile(sv(vars["username"]))
		if p == nil {
			write(w, data(map[string]any{"xig_user_by_username": nil}))
			return
		}
		write(w, data(map[string]any{"xig_user_by_username": profileJSON(p, "http://"+r.Host)}))
	case OpGridFirst:
		p := s.World.Profile(sv(vars["username"]))
		if p == nil {
			write(w, data(map[string]any{"xig_user_by_username": nil}))
			return
		}
		s.writeGrid(w, p, 0, s.pageSize(vars, GridPageSize), id, func(conn any) any {
			return map[string]any{"xig_user_by_username": map[string]any{"pk": str(p.PK), "id": p.ID, "polaris_ordered_timeline_connection": conn}}
		})
	case OpGrid:
		// The connection query resolves the user's id, not its pk: a pk
		// answers a null node, as Instagram does.
		p := s.World.byID[sv(vars["id"])]
		if p == nil {
			write(w, data(map[string]any{"node": nil}))
			return
		}
		pk := p.PK
		offset := 0
		if c := sv(vars["after"]); c != "" {
			var ok bool
			if offset, ok = s.cursor(c, "g", pk, id); !ok {
				softError(w, "invalid cursor")
				return
			}
		} else if s.opts.Faults.NoPKGrid {
			softError(w, "missing cursor")
			return
		}
		s.writeGrid(w, p, offset, s.pageSize(vars, GridPageSize), id, func(conn any) any {
			return map[string]any{"node": map[string]any{"__typename": "XDTUserDict", "polaris_ordered_timeline_connection": conn}}
		})
	case OpComments:
		pk, _ := strconv.ParseInt(sv(vars["media_id"]), 10, 64)
		post := s.World.posts[pk]
		if post == nil || post.Deleted {
			write(w, data(map[string]any{"xig_polaris_media": nil}))
			return
		}
		if a, ok := vars["after"]; ok && a == "" {
			// An empty cursor fails the connection, as on Instagram.
			write(w, map[string]any{"data": map[string]any{"xig_polaris_media": map[string]any{"id": "POLARIS_" + str(pk),
				"comments_connection": map[string]any{"edges": []any{}, "page_info": nil}}},
				"errors": []any{map[string]any{"message": "A server error field_exception occurred. Check server logs for details.",
					"severity": "ERROR", "path": []any{"xig_polaris_media", "comments_connection", "edges"}}}})
			return
		}
		offset := 0
		if c := sv(vars["after"]); c != "" {
			var ok bool
			if offset, ok = s.cursor(c, "c", pk, id); !ok {
				softError(w, "invalid cursor")
				return
			}
		}
		write(w, data(map[string]any{"xig_polaris_media": map[string]any{"pk": str(pk),
			"comments_connection": s.commentPage(post, offset, s.pageSize(vars, CommentPageSize), id.lsd)}}))
	case OpReels:
		p := s.World.byID[sv(vars["id"])]
		if p == nil {
			write(w, data(map[string]any{"node": nil}))
			return
		}
		offset := 0
		if c := sv(vars["after"]); c != "" {
			var ok bool
			if offset, ok = s.cursor(c, "r", p.PK, id); !ok {
				softError(w, "invalid cursor")
				return
			}
		} else if s.opts.Faults.NoCursorReels {
			softError(w, "missing cursor")
			return
		}
		lsd := ""
		if s.opts.Faults.CursorBound {
			lsd = id.lsd
		}
		write(w, data(map[string]any{"node": map[string]any{"__typename": "User", "id": p.ID,
			"polaris_clips_connection": s.reelsPage(p, offset, s.pageSize(vars, ReelsPageSize), lsd)}}))
	case OpHighlights:
		write(w, data(map[string]any{"xdt_api__v1__feed__reels_media__connection": s.highlights(vars["reel_ids"])}))
	default:
		write(w, map[string]any{"errors": []any{map[string]any{"message": "unknown persisted query " + friendly}}})
	}
}

func (s *Server) writeGrid(w http.ResponseWriter, p *Profile, offset, size int, id *ident, wrap func(any) any) {
	if k := s.opts.Faults.GridWallAfter; k > 0 && offset >= k {
		page(w, http.StatusOK, `<html><form action="/accounts/login/ajax/" method="post"></form></html>`)
		return
	}
	lsd := ""
	if s.opts.Faults.CursorBound {
		lsd = id.lsd
	}
	page := s.gridPage(p, offset, size, lsd)
	if !s.opts.Faults.StreamGrid {
		write(w, data(wrap(page)))
		return
	}
	edges, pi := page["edges"].([]any), page["page_info"]
	head := map[string]any{"edges": []any{}, "page_info": map[string]any{"has_next_page": false, "end_cursor": pi.(map[string]any)["end_cursor"]}}
	w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
	enc := json.NewEncoder(w)
	_ = enc.Encode(map[string]any{"data": wrap(head), "extensions": map[string]any{"is_final": false}})
	for i, e := range edges {
		_ = enc.Encode(map[string]any{"data": e, "label": "PolarisProfilePostsTab$stream$" + gridConnKey,
			"path": []any{"xig_user_by_username", gridConnKey, "edges", i}, "extensions": map[string]any{"is_final": false}})
	}
	_ = enc.Encode(map[string]any{"data": map[string]any{"page_info": pi}, "label": "PolarisProfilePostsTab$defer$" + gridConnKey + "_page_info",
		"path": []any{"xig_user_by_username", gridConnKey}, "extensions": map[string]any{"is_final": true}})
}

const gridConnKey = "polaris_ordered_timeline_connection"

// pageSize is the page size a query asked for ("first"), def when it did
// not, capped by the MaxPageSize fault.
func (s *Server) pageSize(vars map[string]any, def int) int {
	n := def
	if v, ok := vars["first"].(json.Number); ok {
		if i, err := v.Int64(); err == nil && i > 0 {
			n = int(i)
		}
	}
	if m := s.opts.Faults.MaxPageSize; m > 0 {
		n = min(n, m)
	}
	return n
}

func (s *Server) gridPage(p *Profile, offset, size int, lsd string) map[string]any {
	grid := p.Grid()
	end := min(offset+size, len(grid))
	edges := []any{}
	for _, post := range grid[min(offset, end):end] {
		edges = append(edges, map[string]any{"node": mediaJSON(post, false)})
	}
	pi := map[string]any{"has_next_page": end < len(grid), "end_cursor": nil}
	if end < len(grid) {
		pi["end_cursor"] = encodeCursor("g", p.PK, end, lsd)
	}
	return map[string]any{"edges": edges, "page_info": pi}
}

// reelsPage is a page of the reels tab: the counts the grid lacks,
// including the view count.
func (s *Server) reelsPage(p *Profile, offset, size int, lsd string) map[string]any {
	reels := p.Reels()
	end := min(offset+size, len(reels))
	edges := []any{}
	for _, x := range reels[min(offset, end):end] {
		edges = append(edges, map[string]any{"node": map[string]any{"__typename": "XIGPolarisVideoMedia", "id": "POLARIS_" + str(x.PK),
			"pk": str(x.PK), "code": x.Code, "product_type": "clips", "like_and_view_counts_disabled": x.LikesHidden,
			"like_count": likes(x), "comment_count": x.CommentCount(), "play_count": x.Plays, "clips_tab_pinned_user_ids": []any{},
			"display_uri": "https://scontent.cdninstagram.example/v/" + x.Code + ".jpg", "user": map[string]any{"pk": str(p.PK), "id": p.ID}}})
	}
	pi := map[string]any{"has_next_page": end < len(reels), "end_cursor": nil}
	if end < len(reels) {
		pi["end_cursor"] = encodeCursor("r", p.PK, end, lsd)
	}
	return map[string]any{"edges": edges, "page_info": pi}
}

func (s *Server) commentPage(post *Post, offset, size int, lsd string) map[string]any {
	if !s.opts.Faults.CursorBound {
		lsd = ""
	}
	end := min(offset+size, len(post.Comments))
	edges := []any{}
	for _, c := range post.Comments[min(offset, end):end] {
		edges = append(edges, map[string]any{"node": map[string]any{"pk": str(c.PK), "id": str(c.PK), "text": c.Text,
			"created_at": c.CreatedAt.Unix(), "comment_like_count": c.Likes, "parent_comment_id": nil, "child_comment_count": c.Replies,
			"user": map[string]any{"pk": str(c.UserPK), "id": str(c.UserPK), "username": c.Username, "is_verified": false}}})
	}
	pi := map[string]any{"has_next_page": end < len(post.Comments), "end_cursor": nil}
	if end < len(post.Comments) {
		pi["end_cursor"] = encodeCursor("c", post.PK, end, lsd)
	}
	return map[string]any{"edges": edges, "page_info": pi}
}

func (s *Server) highlights(ids any) map[string]any {
	edges := []any{}
	list, _ := ids.([]any)
	for _, raw := range list {
		want := strings.TrimPrefix(sv(raw), "highlight:")
		for _, p := range s.World.profiles {
			for _, h := range p.Highlights {
				if str(h.ID) == want {
					items := []any{}
					for i := range h.Items {
						items = append(items, map[string]any{"pk": str(h.ID*10 + int64(i)), "media_type": 1})
					}
					edges = append(edges, map[string]any{"node": map[string]any{"id": "highlight:" + want, "title": h.Title, "items": items}})
				}
			}
		}
	}
	return map[string]any{"edges": edges}
}

func (s *Server) postByCode(code string) *Post {
	for _, post := range s.World.posts {
		if post.Code == code && !post.Deleted {
			return post
		}
	}
	return nil
}

// cursor decodes a cursor of kind for parent pk; a bound cursor must be
// the identity's own.
func (s *Server) cursor(c, kind string, pk int64, id *ident) (int, bool) {
	b, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil {
		return 0, false
	}
	f := strings.Split(string(b), ":")
	if len(f) != 4 || f[0] != kind || f[1] != str(pk) {
		return 0, false
	}
	if s.opts.Faults.CursorBound && f[3] != id.lsd {
		return 0, false
	}
	n, err := strconv.Atoi(f[2])
	return n, err == nil
}

func encodeCursor(kind string, pk int64, offset int, lsd string) string {
	// Real cursors are 90 (grid) and 142 (comments) characters; only the
	// opacity matters here.
	return base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%d:%d:%s", kind, pk, offset, lsd)))
}

func profileJSON(p *Profile, host string) map[string]any {
	hl := []any{}
	for _, h := range p.Highlights {
		hl = append(hl, map[string]any{"node": map[string]any{"id": "highlight:" + str(h.ID), "title": h.Title}})
	}
	return map[string]any{"pk": str(p.PK), "id": p.ID, "username": p.Username, "full_name": p.FullName,
		"biography": p.Bio, "is_verified": p.Verified, "is_private": false, "is_memorialized": false,
		"follower_count": p.Followers, "following_count": p.Following, "all_media_count": nil, "has_any_clips": true,
		"profile_pic_url": host + "/fake-cdn/" + str(p.PK) + ".jpg", "linked_fb_info": nil,
		"bio_links":                 []any{map[string]any{"title": "Site", "url": "https://example.com/" + p.Username}},
		"lox_highlights_connection": map[string]any{"edges": hl}}
}

// mediaJSON is a grid item, or with detail the post page's media object.
func mediaJSON(post *Post, detail bool) map[string]any {
	m := map[string]any{"pk": str(post.PK), "id": str(post.PK) + "_" + str(post.Owner.PK), "code": post.Code,
		"media_type": post.MediaType, "product_type": post.ProductType, "caption": map[string]any{"text": post.Caption},
		"display_uri": "https://scontent.cdninstagram.example/v/" + post.Code + ".jpg",
		"owner":       map[string]any{"pk": str(post.Owner.PK), "id": post.Owner.ID},
		"user":        map[string]any{"username": post.Owner.Username, "id": post.Owner.ID}}
	if detail {
		// The post page's fields, in the shapes of the owner's recordings
		// and probe runs.
		m["taken_at"] = post.TakenAt.Unix()
		m["like_count"] = likes(post)
		m["comment_count"] = post.CommentCount()
		m["like_and_view_counts_disabled"] = post.LikesHidden
		m["image_versions2"] = candidates(post.Code)
		m["caption_is_edited"] = post.Edited
		m["has_audio"] = post.MediaType == 2
		m["original_width"], m["original_height"] = 1080, 1350
		tags := []any{}
		for _, u := range post.Tagged {
			tags = append(tags, map[string]any{"position": []any{0.5, 0.5}, "user": map[string]any{"id": "1", "username": u, "full_name": u, "is_verified": false}})
		}
		m["usertags"] = map[string]any{"in": tags}
		co := []any{}
		for _, u := range post.Coauthors {
			co = append(co, map[string]any{"__typename": "User", "id": "2", "pk": "2", "username": u, "full_name": u, "is_verified": false})
		}
		m["coauthor_producers"] = co
		m["location"] = nil
		if post.Location != "" {
			m["location"] = map[string]any{"__typename": "XDTLocationDict", "pk": 1, "lat": 33.3, "lng": 44.4, "name": post.Location}
		}
		pills := []any{}
		for _, t := range post.Topics {
			pills = append(pills, map[string]any{"seo_canonical_url": nil, "topic_id": strings.ReplaceAll(t, " ", "-"), "topic_name": t})
		}
		m["related_topic_pills"] = pills
		m["ai_label_info"] = nil
		if post.AILabel != "" {
			m["ai_label_info"] = map[string]any{"attribution_label_text": post.AILabel}
		}
		if post.MediaType == 2 {
			m["clips_metadata"] = map[string]any{"music_info": nil, "original_sound_info": map[string]any{"audio_asset_id": "1",
				"original_audio_title": "Original audio", "ig_artist": map[string]any{"username": post.Owner.Username, "id": post.Owner.ID}}}
			m["video_dash_manifest"] = fmt.Sprintf(`<?xml version="1.0"?><MPD mediaPresentationDuration="PT%gS"></MPD>`, post.DurationS)
			m["video_versions"] = []any{map[string]any{"type": 101, "url": "https://scontent.cdninstagram.example/v/" + post.Code + ".mp4"}}
		}
		if post.MediaType == 8 {
			children := []any{}
			for k := range post.CarouselCount {
				code := post.Code + "_" + strconv.Itoa(k)
				children = append(children, map[string]any{"pk": str(post.PK + int64(k) + 1), "media_type": 1,
					"accessibility_caption": "synthetic image " + strconv.Itoa(k+1), "image_versions2": candidates(code)})
			}
			m["carousel_media"] = children
			m["carousel_media_count"] = post.CarouselCount
		}
	}
	return m
}

// likes is the like count the post's pages serve: 3 when hidden, as
// Instagram serves.
func likes(p *Post) int64 {
	if p.LikesHidden {
		return 3
	}
	return p.Likes
}

// candidates are an image's renditions: the full size and a small square.
func candidates(code string) map[string]any {
	base := "https://scontent.cdninstagram.example/v/t51.29350-15/" + code
	return map[string]any{"candidates": []any{
		map[string]any{"url": base + "_n.jpg?stp=dst-jpg_e35&oe=6900AA00&oh=00_fake", "width": 1080, "height": 1350},
		map[string]any{"url": base + "_s150.jpg?stp=c0.135.1080.1080a_s150x150&oe=6900AA00&oh=00_fake", "width": 150, "height": 150},
	}}
}

func data(v any) map[string]any {
	return map[string]any{"data": v, "extensions": map[string]any{"is_final": true}}
}

func write(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
	_ = json.NewEncoder(w).Encode(v)
}

func softError(w http.ResponseWriter, msg string) {
	write(w, map[string]any{"errors": []any{map[string]any{"code": 1357004, "message": msg}}})
}

func page(w http.ResponseWriter, status int, html string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(html))
}

func str(v int64) string { return strconv.FormatInt(v, 10) }

func sv(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case json.Number:
		return t.String()
	}
	return ""
}
