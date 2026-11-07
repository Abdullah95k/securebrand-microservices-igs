package persist

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/common/v1"
	igv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/ig/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/parse"
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
	m := map[string][]time.Time{}
	for _, t := range tables {
		m[t] = times
	}
	return w.withPartitionsFor(ctx, m, fn)
}

// withPartitionsFor is withPartitions with each table's own times.
func (w *Writer) withPartitionsFor(ctx context.Context, times map[string][]time.Time, fn func() error) error {
	err := fn()
	if !store.IsNoPartition(err) {
		return err
	}
	for t, ts := range times {
		for _, at := range ts {
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
						first_seen_at, last_scraped_at, last_profile_read_at, linked_facebook_page_id)
					VALUES ($1, NULLIF($2,''), NULLIF($3,''), NULLIF($4,''), $5, $6, $7, $8, $9, $10, $11, NULLIF($12,''), NULLIF($13,''),
						$14, $15, $16, $17, $18, $19, $20, $20, $20, NULLIF($21,''))
					ON CONFLICT (user_pk) DO UPDATE SET
						username = COALESCE(EXCLUDED.username, t.username), full_name = EXCLUDED.full_name,
						biography = EXCLUDED.biography, bio_links = EXCLUDED.bio_links, verified = EXCLUDED.verified,
						private = EXCLUDED.private, follower_count = EXCLUDED.follower_count,
						following_count = EXCLUDED.following_count, media_count = COALESCE(EXCLUDED.media_count, t.media_count),
						profile_pic_url = EXCLUDED.profile_pic_url, linked_facebook_page = EXCLUDED.linked_facebook_page,
						linked_facebook_page_id = EXCLUDED.linked_facebook_page_id,
						category = EXCLUDED.category, is_business = EXCLUDED.is_business,
						highlight_count = COALESCE(EXCLUDED.highlight_count, t.highlight_count),
						highlights_hash = COALESCE(EXCLUDED.highlights_hash, t.highlights_hash),
						author_key = EXCLUDED.author_key, status = EXCLUDED.status, content_hash = EXCLUDED.content_hash,
						last_scraped_at = EXCLUDED.last_scraped_at, last_profile_read_at = EXCLUDED.last_profile_read_at
					WHERE t.status <> 'suppressed' AND (t.last_profile_read_at IS NULL OR t.last_profile_read_at <= EXCLUDED.last_profile_read_at)`,
					p.UserPk, p.Username, p.FullName, p.Biography, protoJSONList(p.BioLinks), p.IsVerified, p.IsPrivate,
					p.FollowerCount, p.FollowingCount, p.MediaCount, pic, p.LinkedFacebookPage, p.Category, p.IsBusiness, hl,
					emptyNil(p.HighlightsHash), p.AuthorKey, profileStatus(p.Status), hashOf(p.Obs), seen, p.LinkedFacebookPageId)
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
// gone from Instagram only changes its status. A post whose page is walled
// (status_reason login_wall) is stored from the grid's fields, its counts
// NULL, unless it is stored already: then only its status changes
// (REQ-008 fix 15). The reels tab is the only
// source of a reel's views, so a post read from its page keeps the views
// stored, or takes them from the newest metrics sample that has them.
func (w *Writer) Posts(ctx context.Context, posts []*igv1.Post) error {
	posts = latest(posts, func(p *igv1.Post) string { return strconv.FormatInt(p.MediaPk, 10) })
	start := time.Now()
	var rows [][]any
	var gone, walled []*igv1.Post
	var months []time.Time
	var owners [][]any
	for _, p := range posts {
		if p.TakenAt == nil || p.MediaPk == 0 {
			continue
		}
		if p.Status == igv1.PostStatus_POST_STATUS_UNAVAILABLE {
			if p.StatusReason == "login_wall" && p.Shortcode != "" && p.OwnerPk != 0 {
				walled = append(walled, p)
				months = append(months, p.TakenAt.AsTime())
				continue
			}
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
	locked := make([]int64, 0, len(posts))
	for _, p := range posts {
		locked = append(locked, p.MediaPk)
	}
	err := w.withPartitions(ctx, []string{"posts", "comments"}, months, func() error {
		written = 0
		return pgx.BeginFunc(ctx, w.DB, func(tx pgx.Tx) error {
			if err := lockPosts(ctx, tx, locked); err != nil {
				return err
			}
			if len(rows) > 0 {
				if err := copyTemp(ctx, tx, "tmp_posts", "posts", postCols, rows); err != nil {
					return err
				}
				// A newer sighting with another caption: the old one is kept
				// (append, don't overwrite).
				if _, err := tx.Exec(ctx, `INSERT INTO post_edits (media_pk, taken_at, owner_pk, replaced_at, caption)
					SELECT t.media_pk, t.taken_at, t.owner_pk, x.last_seen_at, t.caption FROM tmp_posts x
					JOIN posts t ON t.taken_at = x.taken_at AND t.media_pk = x.media_pk
					WHERE x.caption IS NOT NULL AND t.caption IS NOT NULL AND t.caption IS DISTINCT FROM x.caption
					  AND x.last_seen_at > t.last_seen_at
					ON CONFLICT DO NOTHING`); err != nil {
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
				// Comments stored before the post's row: counted now.
				if _, err := tx.Exec(ctx, `UPDATE posts p SET comments_collected = (SELECT count(*) FROM comments c
						WHERE c.taken_at = p.taken_at AND c.media_pk = p.media_pk AND c.depth = 0 AND c.status = 'visible')
					FROM tmp_posts tp WHERE p.taken_at = tp.taken_at AND p.media_pk = tp.media_pk
					  AND p.comments_collected IS NULL AND p.status = 'visible'`); err != nil {
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
				tag, err := tx.Exec(ctx, `UPDATE posts SET status = 'unavailable', status_reason = $4, last_seen_at = $3
					WHERE taken_at = $1 AND media_pk = $2 AND last_seen_at <= $3`, p.TakenAt.AsTime(), p.MediaPk, observed(p.Obs),
					nilIfEmpty(p.StatusReason))
				if err != nil {
					return err
				}
				written += tag.RowsAffected()
			}
			for _, p := range walled {
				ph := make([]string, len(postCols))
				for i := range ph {
					ph[i] = "$" + strconv.Itoa(i+1)
				}
				tag, err := tx.Exec(ctx, `INSERT INTO posts AS t (`+strings.Join(postCols, ",")+`) VALUES (`+strings.Join(ph, ",")+`)
					ON CONFLICT (taken_at, media_pk) DO UPDATE SET status = 'unavailable', status_reason = EXCLUDED.status_reason,
						last_seen_at = EXCLUDED.last_seen_at
					WHERE t.last_seen_at <= EXCLUDED.last_seen_at`, postRow(p)...)
				if err != nil {
					return err
				}
				written += tag.RowsAffected()
				// Counts the reels tab gave before the post's row existed.
				if _, err := tx.Exec(ctx, `UPDATE posts p SET like_count = CASE WHEN m.counts_hidden THEN NULL ELSE m.like_count END,
						comment_count = m.comment_count, play_count = m.play_count
					FROM (SELECT like_count, comment_count, play_count, counts_hidden FROM post_metrics
						WHERE media_pk = $2 ORDER BY captured_at DESC LIMIT 1) m
					WHERE p.taken_at = $1 AND p.media_pk = $2 AND p.status_reason = 'login_wall'`, p.TakenAt.AsTime(), p.MediaPk); err != nil {
					return err
				}
				if _, err := tx.Exec(ctx, `INSERT INTO post_index (media_pk, owner_pk, taken_at, shortcode) VALUES ($1, $2, $3, $4)
					ON CONFLICT DO NOTHING`, p.MediaPk, p.OwnerPk, p.TakenAt.AsTime(), p.Shortcode); err != nil {
					return err
				}
				if _, err := tx.Exec(ctx, `INSERT INTO post_owners (media_pk, user_pk, role) VALUES ($1, $2, 'owner')
					ON CONFLICT DO NOTHING`, p.MediaPk, p.OwnerPk); err != nil {
					return err
				}
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
				  AND (p.last_seen_at <= m.captured_at
				   -- A walled post's row has no counts of its own: the newest
				   -- sample (from the reels tab) gives them, whenever it comes.
				   OR (p.status_reason = 'login_wall' AND NOT EXISTS (SELECT 1 FROM post_metrics q
						WHERE q.media_pk = m.media_pk AND q.captured_at > m.captured_at)))
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
	// A deletion only marks the stored comment: its event carries no text,
	// which an upsert would wipe.
	var dels []*igv1.Comment
	kept := cs[:0]
	for _, c := range cs {
		if c.Status == igv1.CommentStatus_COMMENT_STATUS_DELETED {
			dels = append(dels, c)
			continue
		}
		kept = append(kept, c)
	}
	cs = kept
	if len(dels) > 0 {
		if _, err := MarkDeleted(ctx, w.DB, dels); err != nil {
			return fmt.Errorf("persist: comments deleted: %w", err)
		}
	}
	if len(cs) == 0 {
		return nil
	}
	start := time.Now()
	rows := make([][]any, 0, len(cs))
	var months []time.Time
	var postTaken []time.Time
	var postMedia []int64
	authors := map[string][]any{}
	for _, c := range cs {
		if c.PostTakenAt == nil || c.CommentPk == 0 {
			continue
		}
		months = append(months, c.PostTakenAt.AsTime())
		postTaken, postMedia = append(postTaken, c.PostTakenAt.AsTime()), append(postMedia, c.MediaPk)
		rows = append(rows, commentRow(c))
		if a := c.Author; a != nil && len(a.AuthorKey) > 0 && !c.Redacted {
			seen := observed(c.Obs)
			authors[string(a.AuthorKey)] = []any{a.AuthorKey, nilIfZero(a.UserPk), nilIfEmpty(a.Username), a.IsVerified,
				nilIfEmpty(a.ProfilePicUrl), seen, seen}
		}
	}
	var written, edited int64
	captured := make([]time.Time, 0, len(cs))
	for _, c := range cs {
		captured = append(captured, observed(c.Obs))
	}
	err := w.withPartitionsFor(ctx, map[string][]time.Time{"comments": months, "comment_metrics": captured}, func() error {
		written, edited = 0, 0
		return pgx.BeginFunc(ctx, w.DB, func(tx pgx.Tx) error {
			if err := lockPosts(ctx, tx, postMedia); err != nil {
				return err
			}
			if err := copyTemp(ctx, tx, "tmp_comments", "comments", commentCols, rows); err != nil {
				return err
			}
			// Before the upsert overwrites them: the text an edit replaced, and
			// the likes and replies of a comment new or changed.
			if _, err := tx.Exec(ctx, `INSERT INTO comment_edits (comment_pk, media_pk, taken_at, owner_pk, author_key, replaced_at, text)
				SELECT t.comment_pk, t.media_pk, t.taken_at, t.owner_pk, t.author_key, x.last_seen_at, t.text FROM tmp_comments x
				JOIN comments t ON t.taken_at = x.taken_at AND t.media_pk = x.media_pk AND t.comment_pk = x.comment_pk
				WHERE NOT x.redacted AND NOT t.redacted AND t.text IS DISTINCT FROM x.text AND x.last_seen_at > t.last_seen_at
				ON CONFLICT DO NOTHING`); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO comment_metrics (comment_pk, captured_at, media_pk, owner_pk, like_count, child_comment_count)
				SELECT x.comment_pk, x.last_seen_at, x.media_pk, x.owner_pk, x.like_count, x.child_comment_count FROM tmp_comments x
				LEFT JOIN comments t ON t.taken_at = x.taken_at AND t.media_pk = x.media_pk AND t.comment_pk = x.comment_pk
				WHERE (x.like_count IS NOT NULL OR x.child_comment_count IS NOT NULL)
				  AND (t.comment_pk IS NULL OR (x.last_seen_at > t.last_seen_at
				   AND (t.like_count IS DISTINCT FROM x.like_count OR t.child_comment_count IS DISTINCT FROM x.child_comment_count)))
				ON CONFLICT DO NOTHING`); err != nil {
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
			if err := r.Err(); err != nil {
				return err
			}
			if err := recount(ctx, tx, postTaken, postMedia); err != nil {
				return err
			}
			if len(authors) == 0 {
				return nil
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

// MarkDeleted sets visible comments to deleted as of their observation. A
// comment seen after that (a later read returns it) becomes visible again
// (Comments). The text and history stay: nothing is removed.
func MarkDeleted(ctx context.Context, db *pgxpool.Pool, cs []*igv1.Comment) (int64, error) {
	taken := make([]time.Time, 0, len(cs))
	media := make([]int64, 0, len(cs))
	ids := make([]int64, 0, len(cs))
	at := make([]time.Time, 0, len(cs))
	for _, c := range cs {
		if c.PostTakenAt == nil || c.CommentPk == 0 {
			continue
		}
		taken = append(taken, c.PostTakenAt.AsTime())
		media = append(media, c.MediaPk)
		ids = append(ids, c.CommentPk)
		at = append(at, observed(c.Obs))
	}
	if len(ids) == 0 {
		return 0, nil
	}
	var n int64
	err := pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		if err := lockPosts(ctx, tx, media); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `UPDATE comments c SET status = 'deleted', deleted_at = d.at
			FROM unnest($1::timestamptz[], $2::bigint[], $3::bigint[], $4::timestamptz[]) AS d(taken, media, id, at)
			WHERE c.taken_at = d.taken AND c.media_pk = d.media AND c.comment_pk = d.id
			  AND c.status = 'visible' AND NOT c.redacted AND c.last_seen_at < d.at`, taken, media, ids, at)
		if err != nil {
			return err
		}
		n = tag.RowsAffected()
		return recount(ctx, tx, taken, media)
	})
	if err != nil {
		return 0, err
	}
	metrics.CommentChanges.WithLabelValues("deleted").Add(float64(n))
	return n, nil
}

// lockPosts takes a transaction lock per post, first thing in a
// transaction, so a post's row and its comments, written by two consumers
// at once, never both miss each other when they count its comments: the
// second waits for the first to commit. Locks are taken in key order; the
// class 1957 keeps them apart from other advisory locks.
func lockPosts(ctx context.Context, tx pgx.Tx, media []int64) error {
	if len(media) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(1957, k) FROM (
		SELECT DISTINCT (m % 2147483647)::int AS k FROM unnest($1::bigint[]) AS m ORDER BY k) s`, media)
	return err
}

// recount sets posts.comments_collected, a post's visible top-level
// comments stored, for the posts given (REQ-008 fix 15). A post not stored
// yet is counted by the finalizer at the job's end.
func recount(ctx context.Context, tx pgx.Tx, taken []time.Time, media []int64) error {
	if len(media) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, `UPDATE posts p SET comments_collected = n.n
		FROM (SELECT d.taken, d.media, (SELECT count(*) FROM comments c WHERE c.taken_at = d.taken AND c.media_pk = d.media
				AND c.depth = 0 AND c.status = 'visible') AS n
			FROM (SELECT DISTINCT * FROM unnest($1::timestamptz[], $2::bigint[])) AS d(taken, media)) n
		WHERE p.taken_at = n.taken AND p.media_pk = n.media AND p.comments_collected IS DISTINCT FROM n.n`, taken, media)
	return err
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
// its items as posts of kind highlight (M12d): new ones added, those still
// there refreshed, those gone marked unavailable (never deleted).
func (w *Writer) Highlights(ctx context.Context, hs []*igv1.Highlight) error {
	hs = latest(hs, func(h *igv1.Highlight) string { return strconv.FormatInt(h.HighlightId, 10) })
	if len(hs) == 0 {
		return nil
	}
	start := time.Now()
	var months []time.Time
	for _, h := range hs {
		for _, it := range h.Items {
			months = append(months, itemTime(h, it))
		}
	}
	err := w.withPartitions(ctx, []string{"posts"}, months, func() error {
		return pgx.BeginFunc(ctx, w.DB, func(tx pgx.Tx) error {
			for _, h := range hs {
				if err := writeHighlight(ctx, tx, h); err != nil {
					return err
				}
			}
			return nil
		})
	})
	if err != nil {
		return fmt.Errorf("persist: highlights: %w", err)
	}
	observe("highlight", start, len(hs), len(hs))
	return nil
}

// itemTime is a highlight item's time: Instagram's, else the time its pk
// encodes (U10), so every sighting of the item lands on the same row.
func itemTime(h *igv1.Highlight, it *igv1.HighlightItem) time.Time {
	if it.TakenAt != nil {
		return it.TakenAt.AsTime()
	}
	if t := parse.TakenAtFromPK(strconv.FormatInt(it.ItemPk, 10)); !t.IsZero() {
		return t
	}
	return time.Unix(0, 0).UTC()
}

func writeHighlight(ctx context.Context, tx pgx.Tx, h *igv1.Highlight) error {
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
		return nil
	}
	link := "https://www.instagram.com/stories/highlights/" + strconv.FormatInt(h.HighlightId, 10) + "/"
	ids := make([]int64, 0, len(h.Items))
	for _, it := range h.Items {
		ids = append(ids, it.ItemPk)
		mt := int16(it.MediaType)
		if mt == 0 {
			mt = 1
		}
		hash := sha256.Sum256([]byte(fmt.Sprintf("%d|%d|%d|%v", h.HighlightId, it.ItemPk, mt, it.MediaIds)))
		if _, err := tx.Exec(ctx, `INSERT INTO posts AS t (taken_at, media_pk, owner_pk, shortcode, media_type, product_type, kind,
				highlight_id, media_ids, permalink, discovered_via, content_hash, status, first_seen_at, last_seen_at)
			VALUES ($1, $2, $3, '', $4, 'story', 'highlight', $5, $6, $7, 'highlight', $8, 'visible', $9, $9)
			ON CONFLICT (taken_at, media_pk) DO UPDATE SET highlight_id = EXCLUDED.highlight_id, media_type = EXCLUDED.media_type,
				media_ids = EXCLUDED.media_ids, permalink = EXCLUDED.permalink, content_hash = EXCLUDED.content_hash,
				status = 'visible', last_seen_at = EXCLUDED.last_seen_at
			WHERE t.last_seen_at <= EXCLUDED.last_seen_at`,
			itemTime(h, it), it.ItemPk, h.UserPk, mt, h.HighlightId, it.MediaIds, link, hash[:], seen); err != nil {
			return err
		}
	}
	_, err := tx.Exec(ctx, `UPDATE posts SET status = 'unavailable', last_seen_at = $3
		WHERE kind = 'highlight' AND highlight_id = $1 AND owner_pk = $4 AND NOT (media_pk = ANY($2)) AND status = 'visible'
		  AND last_seen_at <= $3`, h.HighlightId, ids, seen, h.UserPk)
	return err
}
