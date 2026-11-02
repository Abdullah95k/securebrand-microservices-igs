// Package persist writes igs's entity topics into schema ig: profiles and
// their snapshots, posts with their index and owners, the engagement time
// series, comments and their authors, media metadata and highlights. It is
// fbs's persister rebuilt for Instagram's records (ADR 0005 §4), with the
// same guarantees: idempotent upserts, an older observation never
// overwrites a newer one, erased subjects are never written back, and a
// record that fails on its own data is quarantined instead of stalling
// the batch. Its SQL avoids Postgres 18 features (RETURNING old/new) so it
// runs on any supported Postgres.
package persist

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	igv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/ig/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
)

// Writer performs batched upserts. DB must be a pool on schema ig.
type Writer struct {
	DB *pgxpool.Pool
}

// Batch is one write's worth of decoded entities.
type Batch struct {
	Profiles   []*igv1.Profile
	Posts      []*igv1.Post
	Metrics    []*igv1.PostMetrics
	Comments   []*igv1.Comment
	Media      []*igv1.Media
	Highlights []*igv1.Highlight
}

// Len counts the entities in b.
func (b Batch) Len() int {
	return len(b.Profiles) + len(b.Posts) + len(b.Metrics) + len(b.Comments) + len(b.Media) + len(b.Highlights)
}

// Write applies suppression, then writes profiles, posts, metrics,
// comments, media and highlights, in that order: posts before metrics lets
// a reel's views reach its post row in the same batch.
func (w *Writer) Write(ctx context.Context, b Batch) error {
	b, err := w.Suppress(ctx, b)
	if err != nil {
		return fmt.Errorf("persist: suppression: %w", err)
	}
	steps := []func(context.Context, Batch) error{
		func(ctx context.Context, b Batch) error { return w.Profiles(ctx, b.Profiles) },
		func(ctx context.Context, b Batch) error { return w.Posts(ctx, b.Posts) },
		func(ctx context.Context, b Batch) error { return w.Metrics(ctx, b.Metrics) },
		func(ctx context.Context, b Batch) error { return w.Comments(ctx, b.Comments) },
		func(ctx context.Context, b Batch) error { return w.Media(ctx, b.Media) },
		func(ctx context.Context, b Batch) error { return w.Highlights(ctx, b.Highlights) },
	}
	for _, step := range steps {
		if err := step(ctx, b); err != nil {
			return err
		}
	}
	return nil
}

// Suppress drops what an erasure suppressed since it was published: every
// entity of a suppressed profile, and the media of redacted comments; and
// it redacts the comments of suppressed authors (brief §5 item 4: records
// already in Kafka when an erasure runs must not write erased data back).
func (w *Writer) Suppress(ctx context.Context, b Batch) (Batch, error) {
	profSet, authorSet := map[string]bool{}, map[string]bool{}
	add := func(pk int64) {
		if pk != 0 {
			profSet[strconv.FormatInt(pk, 10)] = true
		}
	}
	for _, p := range b.Profiles {
		add(p.UserPk)
	}
	for _, p := range b.Posts {
		add(p.OwnerPk)
	}
	for _, m := range b.Metrics {
		add(m.OwnerPk)
	}
	for _, c := range b.Comments {
		add(c.OwnerPk)
		if c.Author != nil && len(c.Author.AuthorKey) > 0 {
			authorSet[hex.EncodeToString(c.Author.AuthorKey)] = true
		}
	}
	var mediaComments []int64
	for _, m := range b.Media {
		add(m.UserPk)
		if m.CommentPk != nil {
			mediaComments = append(mediaComments, *m.CommentPk)
		}
	}
	for _, h := range b.Highlights {
		add(h.UserPk)
	}
	if len(profSet) == 0 && len(authorSet) == 0 {
		return b, nil
	}
	rows, err := w.DB.Query(ctx, `SELECT subject_type, subject_key FROM suppression
		WHERE (subject_type IN ('profile', 'page') AND subject_key = ANY($1)) OR (subject_type = 'author' AND subject_key = ANY($2))`,
		keys(profSet), keys(authorSet))
	if err != nil {
		return b, err
	}
	profiles, authors := map[int64]bool{}, map[string]bool{}
	for rows.Next() {
		var typ, k string
		if err := rows.Scan(&typ, &k); err != nil {
			rows.Close()
			return b, err
		}
		if typ == "author" {
			authors[k] = true
		} else {
			pk, _ := strconv.ParseInt(k, 10, 64)
			profiles[pk] = true
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return b, err
	}
	redacted := map[int64]bool{}
	if len(mediaComments) > 0 {
		rows, err := w.DB.Query(ctx, `SELECT comment_pk FROM comments WHERE redacted AND comment_pk = ANY($1)`, mediaComments)
		if err != nil {
			return b, err
		}
		ids, err := pgx.CollectRows(rows, pgx.RowTo[int64])
		if err != nil {
			return b, err
		}
		for _, id := range ids {
			redacted[id] = true
		}
	}
	if len(profiles) == 0 && len(authors) == 0 && len(redacted) == 0 {
		return b, nil
	}
	drop := func(entity string) { metrics.PersistSuppressed.WithLabelValues(entity, "dropped").Inc() }
	out := Batch{}
	for _, p := range b.Profiles {
		if profiles[p.UserPk] {
			drop("profile")
			continue
		}
		out.Profiles = append(out.Profiles, p)
	}
	for _, p := range b.Posts {
		if profiles[p.OwnerPk] {
			drop("post")
			continue
		}
		out.Posts = append(out.Posts, p)
	}
	for _, m := range b.Metrics {
		if profiles[m.OwnerPk] {
			drop("post_metrics")
			continue
		}
		out.Metrics = append(out.Metrics, m)
	}
	for _, c := range b.Comments {
		if profiles[c.OwnerPk] {
			drop("comment")
			continue
		}
		if c.Author != nil && authors[hex.EncodeToString(c.Author.AuthorKey)] && !c.Redacted {
			c.Author = &igv1.Author{AuthorKey: c.Author.AuthorKey}
			c.Text, c.Redacted = "", true
			metrics.PersistSuppressed.WithLabelValues("comment", "redacted").Inc()
		}
		if c.Redacted {
			redacted[c.CommentPk] = true
		}
		out.Comments = append(out.Comments, c)
	}
	for _, m := range b.Media {
		if profiles[m.UserPk] || (m.CommentPk != nil && redacted[*m.CommentPk]) {
			drop("media")
			continue
		}
		out.Media = append(out.Media, m)
	}
	for _, h := range b.Highlights {
		if profiles[h.UserPk] {
			drop("highlight")
			continue
		}
		out.Highlights = append(out.Highlights, h)
	}
	return out, nil
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
