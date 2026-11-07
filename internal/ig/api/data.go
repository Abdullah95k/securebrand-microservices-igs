package api

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/httpapi"
)

// Post is the API view of a stored post.
type Post struct {
	MediaPK              int64           `json:"media_pk"`
	Shortcode            string          `json:"shortcode"`
	Permalink            *string         `json:"permalink,omitempty"`
	OwnerPK              int64           `json:"owner_pk"`
	TakenAt              time.Time       `json:"taken_at"`
	MediaType            int16           `json:"media_type"`
	ProductType          *string         `json:"product_type,omitempty"`
	Caption              *string         `json:"caption,omitempty"`
	CaptionEdited        *bool           `json:"caption_edited,omitempty"`
	Hashtags             []string        `json:"hashtags,omitempty"`
	Mentions             []string        `json:"mentions,omitempty"`
	AccessibilityCaption *string         `json:"accessibility_caption,omitempty"`
	Location             json.RawMessage `json:"location,omitempty"`
	Usertags             json.RawMessage `json:"usertags,omitempty"`
	Coauthors            json.RawMessage `json:"coauthors,omitempty"`
	Music                json.RawMessage `json:"music,omitempty"`
	Topics               []string        `json:"topics,omitempty"`
	AILabel              *string         `json:"ai_label,omitempty"`
	LikeCount            *int64          `json:"like_count,omitempty"`
	CountsHidden         bool            `json:"counts_hidden"`
	CommentCount         *int64          `json:"comment_count,omitempty"`
	CommentingDisabled   *bool           `json:"commenting_disabled,omitempty"`
	PlayCount            *int64          `json:"play_count,omitempty"`
	IsPinned             *bool           `json:"is_pinned,omitempty"`
	ChildCount           *int32          `json:"child_count,omitempty"`
	VideoDurationMS      *int64          `json:"video_duration_ms,omitempty"`
	MediaIDs             []string        `json:"media_ids,omitempty"`
	Status               string          `json:"status"`
	// Why the post is unavailable: login_wall (its page is walled for
	// logged-out readers; only the grid's fields are known) or not_found.
	StatusReason *string `json:"status_reason,omitempty"`
	// Comment coverage, as of the post's last read (REQ-008 fix 15):
	// complete, partial, truncated or none; and the visible top-level
	// comments stored. Logged out, replies are not served.
	CommentsState     string    `json:"comments_state"`
	CommentsCollected *int64    `json:"comments_collected,omitempty"`
	FirstSeenAt       time.Time `json:"first_seen_at"`
	LastSeenAt        time.Time `json:"last_seen_at"`
	// Kind is post, video, reel or highlight (an item of HighlightID).
	Kind        string `json:"kind"`
	HighlightID *int64 `json:"highlight_id,omitempty"`
}

const selectPost = `SELECT media_pk, shortcode, permalink, owner_pk, taken_at, media_type, product_type, caption, caption_edited,
	hashtags, mentions, accessibility_caption, location, usertags, coauthors, music, topics, ai_label, like_count, counts_hidden,
	comment_count, commenting_disabled, play_count, is_pinned, child_count, video_duration_ms, media_ids, status,
	first_seen_at, last_seen_at, kind, highlight_id, status_reason, comments_state, comments_collected FROM posts`

func scanPost(row pgx.CollectableRow) (*Post, error) {
	var p Post
	err := row.Scan(&p.MediaPK, &p.Shortcode, &p.Permalink, &p.OwnerPK, &p.TakenAt, &p.MediaType, &p.ProductType, &p.Caption,
		&p.CaptionEdited, &p.Hashtags, &p.Mentions, &p.AccessibilityCaption, &p.Location, &p.Usertags, &p.Coauthors, &p.Music,
		&p.Topics, &p.AILabel, &p.LikeCount, &p.CountsHidden, &p.CommentCount, &p.CommentingDisabled, &p.PlayCount, &p.IsPinned,
		&p.ChildCount, &p.VideoDurationMS, &p.MediaIDs, &p.Status, &p.FirstSeenAt, &p.LastSeenAt, &p.Kind, &p.HighlightID,
		&p.StatusReason, &p.CommentsState, &p.CommentsCollected)
	return &p, err
}

func timeParam(r *http.Request, name string) (time.Time, bool, error) {
	v := r.URL.Query().Get(name)
	if v == "" {
		return time.Time{}, false, nil
	}
	t, err := time.Parse(time.RFC3339, v)
	return t, err == nil, err
}

// listPosts pages a profile's posts, newest first.
func (s *Server) listPosts(w http.ResponseWriter, r *http.Request) {
	pk, _, ok := s.resolveProfile(w, r)
	if !ok {
		return
	}
	since, hasSince, err1 := timeParam(r, "since")
	until, hasUntil, err2 := timeParam(r, "until")
	if err1 != nil || err2 != nil {
		httpapi.Problem(w, http.StatusBadRequest, "invalid time", "since and until are RFC 3339 times")
		return
	}
	q := selectPost + ` WHERE owner_pk = $1`
	args := []any{pk}
	if k := r.URL.Query().Get("kind"); k != "" {
		switch k {
		case "post", "video", "reel", "highlight":
		default:
			httpapi.Problem(w, http.StatusBadRequest, "invalid kind", "kind is post, video, reel or highlight")
			return
		}
		args = append(args, k)
		q += ` AND kind = $` + strconv.Itoa(len(args))
	}
	if hasSince {
		args = append(args, since)
		q += ` AND taken_at >= $` + strconv.Itoa(len(args))
	}
	if hasUntil {
		args = append(args, until)
		q += ` AND taken_at < $` + strconv.Itoa(len(args))
	}
	if c, ok := httpapi.DecodeCursor(r.URL.Query().Get("cursor")); ok {
		args = append(args, c.T, c.ID)
		q += ` AND (taken_at, media_pk) < ($` + strconv.Itoa(len(args)-1) + `, $` + strconv.Itoa(len(args)) + `)`
	}
	limit := httpapi.LimitParam(r)
	args = append(args, limit)
	q += ` ORDER BY taken_at DESC, media_pk DESC LIMIT $` + strconv.Itoa(len(args))
	rows, err := s.ReadDB.Query(r.Context(), q, args...)
	if err != nil {
		httpapi.ServerError(w, err)
		return
	}
	posts, err := pgx.CollectRows(rows, scanPost)
	if err != nil {
		httpapi.ServerError(w, err)
		return
	}
	if posts == nil {
		posts = []*Post{}
	}
	out := map[string]any{"posts": posts}
	if len(posts) == limit {
		last := posts[len(posts)-1]
		out["next_cursor"] = httpapi.EncodeCursor(last.TakenAt, last.MediaPK)
	}
	httpapi.WriteJSON(w, http.StatusOK, out)
}

// postKey resolves {post} (a shortcode or a media pk) to its pk and time.
func (s *Server) postKey(ctx context.Context, w http.ResponseWriter, r *http.Request) (int64, time.Time, bool) {
	v := chi.URLParam(r, "post")
	var pk int64
	var taken time.Time
	var err error
	if n, perr := strconv.ParseInt(v, 10, 64); perr == nil {
		err = s.ReadDB.QueryRow(ctx, `SELECT media_pk, taken_at FROM post_index WHERE media_pk = $1`, n).Scan(&pk, &taken)
	} else {
		err = s.ReadDB.QueryRow(ctx, `SELECT media_pk, taken_at FROM post_index WHERE shortcode = $1`, v).Scan(&pk, &taken)
	}
	if err != nil {
		httpapi.ServerError(w, err)
		return 0, time.Time{}, false
	}
	return pk, taken, true
}

func (s *Server) getPost(w http.ResponseWriter, r *http.Request) {
	pk, taken, ok := s.postKey(r.Context(), w, r)
	if !ok {
		return
	}
	rows, err := s.ReadDB.Query(r.Context(), selectPost+` WHERE taken_at = $1 AND media_pk = $2`, taken, pk)
	if err != nil {
		httpapi.ServerError(w, err)
		return
	}
	p, err := pgx.CollectExactlyOneRow(rows, scanPost)
	if err != nil {
		httpapi.ServerError(w, err)
		return
	}
	httpapi.WriteJSON(w, http.StatusOK, p)
}

// Comment is the API view of a stored comment. Author identity (pk and
// username) needs the pii:read scope; the author key is always shown.
type Comment struct {
	CommentPK      int64      `json:"comment_pk"`
	MediaPK        int64      `json:"media_pk"`
	ParentPK       *int64     `json:"parent_comment_pk,omitempty"`
	AuthorKey      string     `json:"author_key,omitempty"`
	AuthorPK       *int64     `json:"author_pk,omitempty"`
	AuthorUsername *string    `json:"author_username,omitempty"`
	AuthorVerified *bool      `json:"author_verified,omitempty"`
	Text           *string    `json:"text,omitempty"`
	CreatedAt      *time.Time `json:"created_at,omitempty"`
	LikeCount      *int64     `json:"like_count,omitempty"`
	ReplyCount     *int64     `json:"reply_count,omitempty"`
	IsOP           *bool      `json:"is_op,omitempty"`
	Status         string     `json:"status"`
	Redacted       bool       `json:"redacted"`
	EditCount      int        `json:"edit_count"`
	FirstSeenAt    time.Time  `json:"first_seen_at"`
}

func (s *Server) listComments(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	pk, taken, ok := s.postKey(ctx, w, r)
	if !ok {
		return
	}
	pii := httpapi.ClientFrom(ctx).Has(httpapi.ScopePIIRead)
	q := `SELECT c.comment_pk, c.media_pk, c.parent_comment_pk, c.author_key, c.author_pk, a.username, a.verified, c.text,
		c.created_at, c.like_count, c.child_comment_count, c.is_op, c.status, c.redacted, c.edit_count, c.first_seen_at
		FROM comments c LEFT JOIN comment_authors a ON a.author_key = c.author_key
		WHERE c.taken_at = $1 AND c.media_pk = $2`
	args := []any{taken, pk}
	if cur, ok := httpapi.DecodeCursor(r.URL.Query().Get("cursor")); ok {
		args = append(args, cur.T, cur.ID)
		q += ` AND (coalesce(c.created_at, c.first_seen_at), c.comment_pk) < ($3, $4)`
	}
	limit := httpapi.LimitParam(r)
	args = append(args, limit)
	q += ` ORDER BY coalesce(c.created_at, c.first_seen_at) DESC, c.comment_pk DESC LIMIT $` + strconv.Itoa(len(args))
	rows, err := s.ReadDB.Query(ctx, q, args...)
	if err != nil {
		httpapi.ServerError(w, err)
		return
	}
	cs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*Comment, error) {
		var c Comment
		var key []byte
		err := row.Scan(&c.CommentPK, &c.MediaPK, &c.ParentPK, &key, &c.AuthorPK, &c.AuthorUsername, &c.AuthorVerified, &c.Text,
			&c.CreatedAt, &c.LikeCount, &c.ReplyCount, &c.IsOP, &c.Status, &c.Redacted, &c.EditCount, &c.FirstSeenAt)
		c.AuthorKey = hex.EncodeToString(key)
		if !pii || c.Redacted {
			c.AuthorPK, c.AuthorUsername = nil, nil
		}
		return &c, err
	})
	if err != nil {
		httpapi.ServerError(w, err)
		return
	}
	if cs == nil {
		cs = []*Comment{}
	}
	out := map[string]any{"comments": cs}
	if len(cs) == limit {
		last := cs[len(cs)-1]
		t := last.FirstSeenAt
		if last.CreatedAt != nil {
			t = *last.CreatedAt
		}
		out["next_cursor"] = httpapi.EncodeCursor(t, last.CommentPK)
	}
	httpapi.WriteJSON(w, http.StatusOK, out)
}

// postMetrics returns a post's engagement samples, oldest first.
func (s *Server) postMetrics(w http.ResponseWriter, r *http.Request) {
	pk, _, ok := s.postKey(r.Context(), w, r)
	if !ok {
		return
	}
	rows, err := s.ReadDB.Query(r.Context(), `SELECT captured_at, like_count, comment_count, play_count, counts_hidden
		FROM post_metrics WHERE media_pk = $1 ORDER BY captured_at LIMIT $2`, pk, httpapi.LimitParam(r))
	if err != nil {
		httpapi.ServerError(w, err)
		return
	}
	type sample struct {
		CapturedAt   time.Time `json:"captured_at"`
		LikeCount    *int64    `json:"like_count,omitempty"`
		CommentCount *int64    `json:"comment_count,omitempty"`
		PlayCount    *int64    `json:"play_count,omitempty"`
		CountsHidden bool      `json:"counts_hidden"`
	}
	ms, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (sample, error) {
		var m sample
		err := row.Scan(&m.CapturedAt, &m.LikeCount, &m.CommentCount, &m.PlayCount, &m.CountsHidden)
		return m, err
	})
	if err != nil {
		httpapi.ServerError(w, err)
		return
	}
	if ms == nil {
		ms = []sample{}
	}
	httpapi.WriteJSON(w, http.StatusOK, map[string]any{"metrics": ms})
}

// Media is the API view of a media rendition.
type Media struct {
	MediaID       string     `json:"media_id"`
	SourceMediaID string     `json:"source_media_id"`
	Kind          string     `json:"kind"`
	Variant       string     `json:"variant"`
	MediaPK       *int64     `json:"media_pk,omitempty"`
	ParentMediaPK *int64     `json:"parent_media_pk,omitempty"`
	Position      *int32     `json:"position,omitempty"`
	Width         *int32     `json:"width,omitempty"`
	Height        *int32     `json:"height,omitempty"`
	DurationMS    *int64     `json:"duration_ms,omitempty"`
	AltText       *string    `json:"alt_text,omitempty"`
	Status        string     `json:"status"`
	S3Bucket      *string    `json:"s3_bucket,omitempty"`
	S3Key         *string    `json:"s3_key,omitempty"`
	SourceURL     *string    `json:"source_url,omitempty"`
	SourceExpires *time.Time `json:"source_expires_at,omitempty"`
}

func (s *Server) postMedia(w http.ResponseWriter, r *http.Request) {
	pk, _, ok := s.postKey(r.Context(), w, r)
	if !ok {
		return
	}
	rows, err := s.ReadDB.Query(r.Context(), `SELECT media_id::text, source_media_id, kind, variant, media_pk, parent_media_pk, position,
		width, height, duration_ms, alt_text, status, s3_bucket, s3_key, source_url, source_expires_at
		FROM media WHERE (media_pk = $1 OR parent_media_pk = $1) AND owner_type = 'post' AND status <> 'deleted'
		ORDER BY coalesce(position, 0), variant`, pk)
	if err != nil {
		httpapi.ServerError(w, err)
		return
	}
	ms, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*Media, error) {
		var m Media
		err := row.Scan(&m.MediaID, &m.SourceMediaID, &m.Kind, &m.Variant, &m.MediaPK, &m.ParentMediaPK, &m.Position, &m.Width,
			&m.Height, &m.DurationMS, &m.AltText, &m.Status, &m.S3Bucket, &m.S3Key, &m.SourceURL, &m.SourceExpires)
		return &m, err
	})
	if err != nil {
		httpapi.ServerError(w, err)
		return
	}
	if ms == nil {
		ms = []*Media{}
	}
	httpapi.WriteJSON(w, http.StatusOK, map[string]any{"media": ms})
}

// profileSnapshots returns the profile's history, newest first.
func (s *Server) profileSnapshots(w http.ResponseWriter, r *http.Request) {
	pk, _, ok := s.resolveProfile(w, r)
	if !ok {
		return
	}
	rows, err := s.ReadDB.Query(r.Context(), `SELECT captured_at, username, full_name, follower_count, following_count, media_count,
		verified, private FROM profile_snapshots WHERE user_pk = $1 ORDER BY captured_at DESC LIMIT $2`, pk, httpapi.LimitParam(r))
	if err != nil {
		httpapi.ServerError(w, err)
		return
	}
	type snap struct {
		CapturedAt     time.Time `json:"captured_at"`
		Username       *string   `json:"username,omitempty"`
		FullName       *string   `json:"full_name,omitempty"`
		FollowerCount  *int64    `json:"follower_count,omitempty"`
		FollowingCount *int64    `json:"following_count,omitempty"`
		MediaCount     *int64    `json:"media_count,omitempty"`
		Verified       *bool     `json:"verified,omitempty"`
		Private        *bool     `json:"private,omitempty"`
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (snap, error) {
		var x snap
		err := row.Scan(&x.CapturedAt, &x.Username, &x.FullName, &x.FollowerCount, &x.FollowingCount, &x.MediaCount, &x.Verified, &x.Private)
		return x, err
	})
	if err != nil {
		httpapi.ServerError(w, err)
		return
	}
	if out == nil {
		out = []snap{}
	}
	httpapi.WriteJSON(w, http.StatusOK, map[string]any{"snapshots": out})
}

// profileHighlights returns the profile's highlights with their items.
func (s *Server) profileHighlights(w http.ResponseWriter, r *http.Request) {
	pk, _, ok := s.resolveProfile(w, r)
	if !ok {
		return
	}
	rows, err := s.ReadDB.Query(r.Context(), `SELECT h.highlight_id, h.title, h.cover_url, h.item_count, h.status, h.last_seen_at,
		coalesce((SELECT json_agg(json_build_object('item_pk', i.media_pk, 'taken_at', i.taken_at, 'media_type', i.media_type,
			'media_ids', i.media_ids) ORDER BY i.media_pk) FROM posts i
			WHERE i.kind = 'highlight' AND i.highlight_id = h.highlight_id AND i.owner_pk = h.user_pk AND i.status = 'visible'), '[]')
		FROM highlights h WHERE h.user_pk = $1 ORDER BY h.highlight_id`, pk)
	if err != nil {
		httpapi.ServerError(w, err)
		return
	}
	type hl struct {
		ID         int64           `json:"highlight_id"`
		Title      *string         `json:"title,omitempty"`
		CoverURL   *string         `json:"cover_url,omitempty"`
		ItemCount  *int32          `json:"item_count,omitempty"`
		Status     string          `json:"status"`
		LastSeenAt time.Time       `json:"last_seen_at"`
		Items      json.RawMessage `json:"items"`
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (hl, error) {
		var x hl
		err := row.Scan(&x.ID, &x.Title, &x.CoverURL, &x.ItemCount, &x.Status, &x.LastSeenAt, &x.Items)
		return x, err
	})
	if err != nil {
		httpapi.ServerError(w, err)
		return
	}
	if out == nil {
		out = []hl{}
	}
	httpapi.WriteJSON(w, http.StatusOK, map[string]any{"highlights": out})
}
