package persist

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/common/v1"
	igv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/ig/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/store"
)

// latest keeps the most recently observed message per key.
func latest[T interface{ GetObs() *commonv1.Observation }](xs []T, key func(T) string) []T {
	idx := map[string]int{}
	out := make([]T, 0, len(xs))
	for _, x := range xs {
		k := key(x)
		if i, ok := idx[k]; ok {
			if !observed(x.GetObs()).Before(observed(out[i].GetObs())) {
				out[i] = x
			}
			continue
		}
		idx[k] = len(out)
		out = append(out, x)
	}
	return out
}

// withPartitions runs fn and, if a partition was missing, creates the
// partitions of tables holding times, then runs it once more.
func (w *Writer) withPartitions(ctx context.Context, tables []string, times []time.Time, fn func() error) error {
	err := fn()
	if !store.IsNoPartition(err) {
		return err
	}
	for _, t := range tables {
		for _, at := range times {
			if perr := store.IGLayout.EnsurePartitionFor(ctx, w.DB, t, at); perr != nil {
				return errors.Join(err, perr)
			}
		}
	}
	return fn()
}

func copyTemp(ctx context.Context, tx pgx.Tx, tmp, like string, cols []string, rows [][]any) error {
	if _, err := tx.Exec(ctx, fmt.Sprintf("CREATE TEMP TABLE %s (LIKE %s INCLUDING DEFAULTS) ON COMMIT DROP", tmp, like)); err != nil {
		return err
	}
	_, err := tx.CopyFrom(ctx, pgx.Identifier{tmp}, cols, pgx.CopyFromRows(rows))
	return err
}

func setClause(cols []string, skip ...string) []string {
	out := make([]string, 0, len(cols))
	for _, c := range cols {
		if !slices.Contains(skip, c) {
			out = append(out, c+" = EXCLUDED."+c)
		}
	}
	return out
}

func observe(entity string, start time.Time, written, total int) {
	metrics.PersistRows.WithLabelValues(entity, "written").Add(float64(written))
	metrics.PersistRows.WithLabelValues(entity, "unchanged").Add(float64(max(total-written, 0)))
	metrics.PersistBatch.WithLabelValues(entity).Observe(time.Since(start).Seconds())
}

// Profiles upserts profiles and appends a snapshot when the counts, name or
// flags changed. A suppressed profile is never written; an unavailable one
// only changes its status. Usernames move between accounts (D8): a name
// taken by this profile is cleared from any other row that still holds it.
func (w *Writer) Profiles(ctx context.Context, ps []*igv1.Profile) error {
	ps = latest(ps, func(p *igv1.Profile) string { return strconv.FormatInt(p.UserPk, 10) })
	start := time.Now()
	written := 0
	for _, p := range ps {
		if p.UserPk == 0 {
			continue
		}
		seen := observed(p.Obs)
		wrote := false
		err := w.withPartitions(ctx, []string{"profile_snapshots"}, []time.Time{seen}, func() error {
			wrote = false
			return pgx.BeginFunc(ctx, w.DB, func(tx pgx.Tx) error {
				if p.Status == igv1.ProfileStatus_PROFILE_STATUS_UNAVAILABLE {
					_, err := tx.Exec(ctx, `UPDATE profiles SET status = 'unavailable', last_scraped_at = $2
						WHERE user_pk = $1 AND status <> 'suppressed' AND (last_scraped_at IS NULL OR last_scraped_at <= $2)`, p.UserPk, seen)
					return err
				}
				if p.Username != "" {
					if _, err := tx.Exec(ctx, `UPDATE profiles SET username = NULL
						WHERE lower(username) = lower($1) AND user_pk <> $2`, p.Username, p.UserPk); err != nil {
						return err
					}
				}
				var pic *string
				if p.ProfilePic != nil {
					pic = nilIfEmpty(p.ProfilePic.Url)
				}
				var hl *int32
				if p.HighlightCount != nil {
					hl = p.HighlightCount
				}
				tag, err := tx.Exec(ctx, `
					INSERT INTO profiles AS t (user_pk, username, full_name, biography, bio_links, verified, private,
						follower_count, following_count, media_count, profile_pic_url, linked_facebook_page, category,
						is_business, highlight_count, highlights_hash, author_key, status, content_hash,
						first_seen_at, last_scraped_at, last_profile_read_at)
					VALUES ($1, NULLIF($2,''), NULLIF($3,''), NULLIF($4,''), $5, $6, $7, $8, $9, $10, $11, NULLIF($12,''), NULLIF($13,''),
						$14, $15, $16, $17, $18, $19, $20, $20, $20)
					ON CONFLICT (user_pk) DO UPDATE SET
						username = COALESCE(EXCLUDED.username, t.username), full_name = EXCLUDED.full_name,
						biography = EXCLUDED.biography, bio_links = EXCLUDED.bio_links, verified = EXCLUDED.verified,
						private = EXCLUDED.private, follower_count = EXCLUDED.follower_count,
						following_count = EXCLUDED.following_count, media_count = COALESCE(EXCLUDED.media_count, t.media_count),
						profile_pic_url = EXCLUDED.profile_pic_url, linked_facebook_page = EXCLUDED.linked_facebook_page,
						category = EXCLUDED.category, is_business = EXCLUDED.is_business,
						highlight_count = COALESCE(EXCLUDED.highlight_count, t.highlight_count),
						highlights_hash = COALESCE(EXCLUDED.highlights_hash, t.highlights_hash),
						author_key = EXCLUDED.author_key, status = EXCLUDED.status, content_hash = EXCLUDED.content_hash,
						last_scraped_at = EXCLUDED.last_scraped_at, last_profile_read_at = EXCLUDED.last_profile_read_at
					WHERE t.status <> 'suppressed' AND (t.last_profile_read_at IS NULL OR t.last_profile_read_at <= EXCLUDED.last_profile_read_at)`,
					p.UserPk, p.Username, p.FullName, p.Biography, protoJSONList(p.BioLinks), p.IsVerified, p.IsPrivate,
					p.FollowerCount, p.FollowingCount, p.MediaCount, pic, p.LinkedFacebookPage, p.Category, p.IsBusiness, hl,
					emptyNil(p.HighlightsHash), p.AuthorKey, profileStatus(p.Status), hashOf(p.Obs), seen)
				if err != nil || tag.RowsAffected() == 0 {
					return err
				}
				wrote = true
				// A snapshot only when something it records changed since
				// the latest one.
				_, err = tx.Exec(ctx, `
					INSERT INTO profile_snapshots (user_pk, captured_at, username, full_name, follower_count, following_count,
						media_count, verified, private)
					SELECT $1, $2, NULLIF($3,''), NULLIF($4,''), $5, $6, $7, $8, $9
					WHERE NOT EXISTS (
						SELECT 1 FROM (SELECT * FROM profile_snapshots WHERE user_pk = $1 AND captured_at <= $2
							ORDER BY captured_at DESC LIMIT 1) l
						WHERE l.username IS NOT DISTINCT FROM NULLIF($3,'') AND l.full_name IS NOT DISTINCT FROM NULLIF($4,'')
						  AND l.follower_count IS NOT DISTINCT FROM $5 AND l.following_count IS NOT DISTINCT FROM $6
						  AND l.media_count IS NOT DISTINCT FROM $7 AND l.verified IS NOT DISTINCT FROM $8
						  AND l.private IS NOT DISTINCT FROM $9)
					ON CONFLICT DO NOTHING`,
					p.UserPk, seen, p.Username, p.FullName, p.FollowerCount, p.FollowingCount, p.MediaCount, p.IsVerified, p.IsPrivate)
				return err
			})
		})
		if err != nil {
			return fmt.Errorf("persist: profiles: %w", err)
		}
		if wrote {
			written++
		}
	}
	observe("profile", start, written, len(ps))
	return nil
}

func emptyNil(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return b
}

// Posts upserts posts and maintains post_index and post_owners. A post
// gone from Instagram only changes its status. The reels tab is the only
// source of a reel's views, so a post read from its page keeps the views
// stored, or takes them from the newest metrics sample that has them.
func (w *Writer) Posts(ctx context.Context, posts []*igv1.Post) error {
	posts = latest(posts, func(p *igv1.Post) string { return strconv.FormatInt(p.MediaPk, 10) })
	start := time.Now()
	var rows [][]any
	var gone []*igv1.Post
	var months []time.Time
	var owners [][]any
	for _, p := range posts {
		if p.TakenAt == nil || p.MediaPk == 0 {
			continue
		}
		if p.Status == igv1.PostStatus_POST_STATUS_UNAVAILABLE {
			gone = append(gone, p)
			continue
		}
		rows = append(rows, postRow(p))
		months = append(months, p.TakenAt.AsTime())
		owners = append(owners, []any{p.MediaPk, p.OwnerPk, "owner"})
		for _, c := range p.Coauthors {
			if c.UserPk != 0 && c.UserPk != p.OwnerPk {
				owners = append(owners, []any{p.MediaPk, c.UserPk, "coauthor"})
			}
		}
	}
	var written int64
	err := w.withPartitions(ctx, []string{"posts", "comments"}, months, func() error {
		written = 0
		return pgx.BeginFunc(ctx, w.DB, func(tx pgx.Tx) error {
			if len(rows) > 0 {
				if err := copyTemp(ctx, tx, "tmp_posts", "posts", postCols, rows); err != nil {
					return err
				}
				set := setClause(postCols, "taken_at", "media_pk", "first_seen_at", "play_count", "is_pinned")
				set = append(set, "play_count = COALESCE(EXCLUDED.play_count, t.play_count)",
					"is_pinned = COALESCE(EXCLUDED.is_pinned, t.is_pinned)")
				tag, err := tx.Exec(ctx, `INSERT INTO posts AS t (`+strings.Join(postCols, ",")+`)
					SELECT `+strings.Join(postCols, ",")+` FROM tmp_posts
					ON CONFLICT (taken_at, media_pk) DO UPDATE SET `+strings.Join(set, ", ")+`
					WHERE t.last_seen_at <= EXCLUDED.last_seen_at
					  AND (t.content_hash IS DISTINCT FROM EXCLUDED.content_hash OR t.status <> 'visible'
					   OR t.last_seen_at < EXCLUDED.last_seen_at - interval '1 day')`)
				if err != nil {
					return err
				}
				written = tag.RowsAffected()
				if _, err := tx.Exec(ctx, `INSERT INTO post_index (media_pk, owner_pk, taken_at, shortcode)
					SELECT media_pk, owner_pk, taken_at, shortcode FROM tmp_posts ON CONFLICT DO NOTHING`); err != nil {
					return err
				}
				// Views read from the reels tab before the post's page.
				if _, err := tx.Exec(ctx, `UPDATE posts p SET play_count = m.play_count
					FROM tmp_posts tp, LATERAL (SELECT play_count FROM post_metrics pm
						WHERE pm.media_pk = tp.media_pk AND pm.play_count IS NOT NULL ORDER BY captured_at DESC LIMIT 1) m
					WHERE p.taken_at = tp.taken_at AND p.media_pk = tp.media_pk AND p.play_count IS NULL`); err != nil {
					return err
				}
			}
			if len(owners) > 0 {
				if _, err := tx.Exec(ctx, `INSERT INTO post_owners (media_pk, user_pk, role)
					SELECT * FROM unnest($1::bigint[], $2::bigint[], $3::text[]) ON CONFLICT DO NOTHING`,
					column[int64](owners, 0), column[int64](owners, 1), column[string](owners, 2)); err != nil {
					return err
				}
			}
			for _, p := range gone {
				tag, err := tx.Exec(ctx, `UPDATE posts SET status = 'unavailable', last_seen_at = $3
					WHERE taken_at = $1 AND media_pk = $2 AND last_seen_at <= $3`, p.TakenAt.AsTime(), p.MediaPk, observed(p.Obs))
				if err != nil {
					return err
				}
				written += tag.RowsAffected()
			}
			return nil
		})
	})
	if err != nil {
		return fmt.Errorf("persist: posts: %w", err)
	}
	observe("post", start, int(written), len(posts))
	return nil
}

func column[T any](rows [][]any, i int) []T {
	out := make([]T, len(rows))
	for j, r := range rows {
		out[j] = r[i].(T)
	}
	return out
}

// Metrics appends an engagement sample when its counts differ from the
// newest stored one (a count a sample lacks, such as a post page's views,
// carries the stored one forward), and brings the post row's counts up to
// date with it.
func (w *Writer) Metrics(ctx context.Context, ms []*igv1.PostMetrics) error {
	if len(ms) == 0 {
		return nil
	}
	start := time.Now()
	// Samples of one post in one batch merge in time order: the later
	// sample's counts win, its missing ones are filled from the earlier.
	slices.SortStableFunc(ms, func(a, b *igv1.PostMetrics) int {
		return a.GetCapturedAt().AsTime().Compare(b.GetCapturedAt().AsTime())
	})
	merged := map[int64]*igv1.PostMetrics{}
	var order []int64
	for _, m := range ms {
		if m.MediaPk == 0 || m.CapturedAt == nil {
			continue
		}
		cur, ok := merged[m.MediaPk]
		if !ok {
			order = append(order, m.MediaPk)
			merged[m.MediaPk] = proto.Clone(m).(*igv1.PostMetrics)
			continue
		}
		if m.LikeCount != nil || m.CountsHidden {
			cur.LikeCount = m.LikeCount
		}
		if m.CommentCount != nil {
			cur.CommentCount = m.CommentCount
		}
		if m.PlayCount != nil {
			cur.PlayCount = m.PlayCount
		}
		cur.CountsHidden = m.CountsHidden
		cur.CapturedAt = m.CapturedAt
		if cur.TakenAt == nil {
			cur.TakenAt = m.TakenAt
		}
	}
	rows := make([][]any, 0, len(order))
	var days []time.Time
	for _, pk := range order {
		m := merged[pk]
		at := m.CapturedAt.AsTime()
		rows = append(rows, []any{m.MediaPk, at, m.OwnerPk, m.LikeCount, m.CommentCount, m.PlayCount, m.CountsHidden, ts(m.TakenAt)})
		days = append(days, at)
	}
	var written int64
	err := w.withPartitions(ctx, []string{"post_metrics"}, days, func() error {
		written = 0
		return pgx.BeginFunc(ctx, w.DB, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, `CREATE TEMP TABLE tmp_pm (media_pk bigint, captured_at timestamptz, owner_pk bigint,
				like_count bigint, comment_count bigint, play_count bigint, counts_hidden boolean, taken_at timestamptz) ON COMMIT DROP`); err != nil {
				return err
			}
			if _, err := tx.CopyFrom(ctx, pgx.Identifier{"tmp_pm"}, []string{"media_pk", "captured_at", "owner_pk", "like_count",
				"comment_count", "play_count", "counts_hidden", "taken_at"}, pgx.CopyFromRows(rows)); err != nil {
				return err
			}
			// Carry the newest stored counts into what a sample lacks.
			if _, err := tx.Exec(ctx, `UPDATE tmp_pm m SET (like_count, comment_count, play_count) = (
					SELECT CASE WHEN m.like_count IS NULL AND NOT m.counts_hidden THEN l.like_count ELSE m.like_count END,
						COALESCE(m.comment_count, l.comment_count), COALESCE(m.play_count, l.play_count)
					FROM (SELECT like_count, comment_count, play_count FROM post_metrics p
						WHERE p.media_pk = m.media_pk AND p.captured_at < m.captured_at ORDER BY captured_at DESC LIMIT 1) l)
				WHERE EXISTS (SELECT 1 FROM post_metrics p WHERE p.media_pk = m.media_pk AND p.captured_at < m.captured_at)`); err != nil {
				return err
			}
			tag, err := tx.Exec(ctx, `INSERT INTO post_metrics (media_pk, captured_at, owner_pk, like_count, comment_count, play_count, counts_hidden)
				SELECT m.media_pk, m.captured_at, m.owner_pk, m.like_count, m.comment_count, m.play_count, m.counts_hidden FROM tmp_pm m
				WHERE NOT EXISTS (SELECT 1 FROM (SELECT * FROM post_metrics p WHERE p.media_pk = m.media_pk AND p.captured_at <= m.captured_at
						ORDER BY captured_at DESC LIMIT 1) l
					WHERE l.like_count IS NOT DISTINCT FROM m.like_count AND l.comment_count IS NOT DISTINCT FROM m.comment_count
					  AND l.play_count IS NOT DISTINCT FROM m.play_count AND l.counts_hidden = m.counts_hidden)
				ON CONFLICT DO NOTHING`)
			if err != nil {
				return err
			}
			written = tag.RowsAffected()
			// The post row shows the newest counts.
			_, err = tx.Exec(ctx, `UPDATE posts p SET
					like_count = CASE WHEN m.counts_hidden THEN NULL ELSE COALESCE(m.like_count, p.like_count) END,
					comment_count = COALESCE(m.comment_count, p.comment_count),
					play_count = COALESCE(m.play_count, p.play_count)
				FROM tmp_pm m
				WHERE m.taken_at IS NOT NULL AND p.taken_at = m.taken_at AND p.media_pk = m.media_pk
				  AND p.last_seen_at <= m.captured_at
				  AND (p.like_count IS DISTINCT FROM CASE WHEN m.counts_hidden THEN NULL ELSE COALESCE(m.like_count, p.like_count) END
				   OR p.comment_count IS DISTINCT FROM COALESCE(m.comment_count, p.comment_count)
				   OR p.play_count IS DISTINCT FROM COALESCE(m.play_count, p.play_count))`)
			return err
		})
	})
	if err != nil {
		return fmt.Errorf("persist: post_metrics: %w", err)
	}
	observe("post_metrics", start, int(written), len(order))
	return nil
}

// Comments upserts comments and their authors. An observation older than
// the stored one changes nothing; a newer one with other text counts as an
// edit; a redacted comment stays redacted.
func (w *Writer) Comments(ctx context.Context, cs []*igv1.Comment) error {
	cs = latest(cs, func(c *igv1.Comment) string { return strconv.FormatInt(c.CommentPk, 10) })
	if len(cs) == 0 {
		return nil
	}
	start := time.Now()
	rows := make([][]any, 0, len(cs))
	var months []time.Time
	authors := map[string][]any{}
	for _, c := range cs {
		if c.PostTakenAt == nil || c.CommentPk == 0 {
			continue
		}
		months = append(months, c.PostTakenAt.AsTime())
		rows = append(rows, commentRow(c))
		if a := c.Author; a != nil && len(a.AuthorKey) > 0 && !c.Redacted {
			seen := observed(c.Obs)
			authors[string(a.AuthorKey)] = []any{a.AuthorKey, nilIfZero(a.UserPk), nilIfEmpty(a.Username), a.IsVerified,
				nilIfEmpty(a.ProfilePicUrl), seen, seen}
		}
	}
	var written, edited int64
	err := w.withPartitions(ctx, []string{"comments"}, months, func() error {
		written, edited = 0, 0
		return pgx.BeginFunc(ctx, w.DB, func(tx pgx.Tx) error {
			if err := copyTemp(ctx, tx, "tmp_comments", "comments", commentCols, rows); err != nil {
				return err
			}
			const isEdit = `(NOT EXCLUDED.redacted AND t.text IS DISTINCT FROM EXCLUDED.text)`
			set := setClause(commentCols, "taken_at", "media_pk", "comment_pk", "first_seen_at", "status")
			set = append(set,
				"edit_count = t.edit_count + CASE WHEN "+isEdit+" THEN 1 ELSE 0 END",
				"edited_at = CASE WHEN "+isEdit+" THEN EXCLUDED.last_seen_at ELSE t.edited_at END",
				"status = CASE WHEN t.status = 'deleted' AND EXCLUDED.last_seen_at > t.deleted_at THEN 'visible' ELSE t.status END",
				"deleted_at = CASE WHEN t.status = 'deleted' AND EXCLUDED.last_seen_at > t.deleted_at THEN NULL ELSE t.deleted_at END")
			r, err := tx.Query(ctx, `INSERT INTO comments AS t (`+strings.Join(commentCols, ",")+`)
				SELECT `+strings.Join(commentCols, ",")+` FROM tmp_comments
				ON CONFLICT (taken_at, media_pk, comment_pk) DO UPDATE SET `+strings.Join(set, ", ")+`
				WHERE NOT t.redacted AND EXCLUDED.last_seen_at >= t.last_seen_at
				  AND (t.content_hash IS DISTINCT FROM EXCLUDED.content_hash OR t.status = 'deleted'
				   OR t.last_seen_at < EXCLUDED.last_seen_at - interval '1 day')
				RETURNING edit_count > 0 AND edited_at = last_seen_at`)
			if err != nil {
				return err
			}
			for r.Next() {
				var e bool
				if err := r.Scan(&e); err != nil {
					r.Close()
					return err
				}
				written++
				if e {
					edited++
				}
			}
			r.Close()
			if err := r.Err(); err != nil || len(authors) == 0 {
				return err
			}
			arows := make([][]any, 0, len(authors))
			for _, a := range authors {
				arows = append(arows, a)
			}
			acols := []string{"author_key", "author_pk", "username", "verified", "profile_pic_url", "first_seen_at", "last_seen_at"}
			if err := copyTemp(ctx, tx, "tmp_authors", "comment_authors", acols, arows); err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `INSERT INTO comment_authors AS a SELECT * FROM tmp_authors
				ON CONFLICT (author_key) DO UPDATE SET author_pk = COALESCE(EXCLUDED.author_pk, a.author_pk),
					username = COALESCE(EXCLUDED.username, a.username), verified = EXCLUDED.verified,
					profile_pic_url = COALESCE(EXCLUDED.profile_pic_url, a.profile_pic_url),
					last_seen_at = GREATEST(a.last_seen_at, EXCLUDED.last_seen_at)
				WHERE a.last_seen_at < EXCLUDED.last_seen_at - interval '1 day'
				   OR a.username IS DISTINCT FROM COALESCE(EXCLUDED.username, a.username)
				   OR a.verified IS DISTINCT FROM EXCLUDED.verified`)
			return err
		})
	})
	if err != nil {
		return fmt.Errorf("persist: comments: %w", err)
	}
	observe("comment", start, int(written), len(rows))
	metrics.CommentChanges.WithLabelValues("edited").Add(float64(edited))
	return nil
}

// Media upserts media metadata. A newer sighting refreshes the signed
// source URL; the download state is the media services'.
func (w *Writer) Media(ctx context.Context, ms []*igv1.Media) error {
	ms = latest(ms, func(m *igv1.Media) string { return m.MediaId })
	if len(ms) == 0 {
		return nil
	}
	start := time.Now()
	rows := make([][]any, 0, len(ms))
	for _, m := range ms {
		rows = append(rows, mediaRow(m))
	}
	var written int64
	err := pgx.BeginFunc(ctx, w.DB, func(tx pgx.Tx) error {
		if err := copyTemp(ctx, tx, "tmp_media", "media", mediaCols, rows); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `INSERT INTO media AS t (`+strings.Join(mediaCols, ",")+`)
			SELECT `+strings.Join(mediaCols, ",")+` FROM tmp_media
			ON CONFLICT (media_id) DO UPDATE SET source_url = EXCLUDED.source_url, source_expires_at = EXCLUDED.source_expires_at,
				width = COALESCE(EXCLUDED.width, t.width), height = COALESCE(EXCLUDED.height, t.height),
				duration_ms = COALESCE(EXCLUDED.duration_ms, t.duration_ms), alt_text = COALESCE(EXCLUDED.alt_text, t.alt_text)
			WHERE t.status NOT IN ('deleted') AND t.source_url IS DISTINCT FROM EXCLUDED.source_url
			  AND (t.source_expires_at IS NULL OR EXCLUDED.source_expires_at IS NULL OR t.source_expires_at <= EXCLUDED.source_expires_at)`)
		written = tag.RowsAffected()
		return err
	})
	if err != nil {
		return fmt.Errorf("persist: media: %w", err)
	}
	observe("media", start, int(written), len(ms))
	return nil
}

// Highlights upserts highlights and, when a highlight came with its items,
// replaces its items.
func (w *Writer) Highlights(ctx context.Context, hs []*igv1.Highlight) error {
	hs = latest(hs, func(h *igv1.Highlight) string { return strconv.FormatInt(h.HighlightId, 10) })
	if len(hs) == 0 {
		return nil
	}
	start := time.Now()
	err := pgx.BeginFunc(ctx, w.DB, func(tx pgx.Tx) error {
		for _, h := range hs {
			var cover *string
			if h.Cover != nil {
				cover = nilIfEmpty(h.Cover.Url)
			}
			var n *int32
			if h.ItemCount > 0 {
				n = &h.ItemCount
			} else if len(h.Items) > 0 {
				c := int32(len(h.Items))
				n = &c
			}
			seen := observed(h.Obs)
			if _, err := tx.Exec(ctx, `INSERT INTO highlights AS t (highlight_id, user_pk, title, cover_url, item_count, status, first_seen_at, last_seen_at)
				VALUES ($1, $2, NULLIF($3,''), $4, $5, 'visible', $6, $6)
				ON CONFLICT (highlight_id) DO UPDATE SET title = EXCLUDED.title, cover_url = COALESCE(EXCLUDED.cover_url, t.cover_url),
					item_count = COALESCE(EXCLUDED.item_count, t.item_count), status = 'visible', last_seen_at = EXCLUDED.last_seen_at
				WHERE t.last_seen_at <= EXCLUDED.last_seen_at`, h.HighlightId, h.UserPk, h.Title, cover, n, seen); err != nil {
				return err
			}
			if len(h.Items) == 0 {
				continue
			}
			ids := make([]int64, 0, len(h.Items))
			for _, it := range h.Items {
				ids = append(ids, it.ItemPk)
				if _, err := tx.Exec(ctx, `INSERT INTO highlight_items (highlight_id, item_pk, taken_at, media_type, media_ids)
					VALUES ($1, $2, $3, $4, $5) ON CONFLICT (highlight_id, item_pk) DO UPDATE SET taken_at = EXCLUDED.taken_at,
						media_type = EXCLUDED.media_type, media_ids = EXCLUDED.media_ids`,
					h.HighlightId, it.ItemPk, ts(it.TakenAt), int16(it.MediaType), it.MediaIds); err != nil {
					return err
				}
			}
			if _, err := tx.Exec(ctx, `DELETE FROM highlight_items WHERE highlight_id = $1 AND NOT (item_pk = ANY($2))`, h.HighlightId, ids); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("persist: highlights: %w", err)
	}
	observe("highlight", start, len(hs), len(hs))
	return nil
}
