package persist

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	fbv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/fb/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
)

// Batch is one write's worth of decoded entities.
type Batch struct {
	Pages    []*fbv1.Page
	Posts    []*fbv1.Post
	Comments []*fbv1.Comment
	Media    []*fbv1.Media
}

// Len counts the entities in b.
func (b Batch) Len() int { return len(b.Pages) + len(b.Posts) + len(b.Comments) + len(b.Media) }

// Write applies suppression (Suppress), then writes pages, posts, comments
// and media, in that order: posts before comments keeps post_index ahead of
// comment lookups.
func (w *Writer) Write(ctx context.Context, b Batch) error {
	b, err := w.Suppress(ctx, b)
	if err != nil {
		return fmt.Errorf("persist: suppression: %w", err)
	}
	if err := w.Pages(ctx, b.Pages); err != nil {
		return err
	}
	if err := w.Posts(ctx, b.Posts); err != nil {
		return err
	}
	if err := w.Comments(ctx, b.Comments); err != nil {
		return err
	}
	if err := w.Media(ctx, b.Media); err != nil {
		return fmt.Errorf("media: %w", err)
	}
	return nil
}

// Suppress drops what an erasure suppressed since it was published: every
// entity of a suppressed page, and the media of redacted comments; and it
// redacts the comments of suppressed authors. The crawlers check
// suppression before publishing, but records already in Kafka when an
// erasure runs would otherwise write erased data back (brief §5 item 4).
func (w *Writer) Suppress(ctx context.Context, b Batch) (Batch, error) {
	pageSet, authorSet := map[string]bool{}, map[string]bool{}
	addPage := func(id int64) {
		if id != 0 {
			pageSet[strconv.FormatInt(id, 10)] = true
		}
	}
	for _, p := range b.Pages {
		addPage(p.PageId)
	}
	for _, p := range b.Posts {
		addPage(p.PageId)
	}
	for _, c := range b.Comments {
		addPage(c.PageId)
		if c.Author != nil && len(c.Author.AuthorKey) > 0 {
			authorSet[hex.EncodeToString(c.Author.AuthorKey)] = true
		}
	}
	var mediaComments []int64
	for _, m := range b.Media {
		addPage(m.PageId)
		if m.CommentId != nil {
			mediaComments = append(mediaComments, *m.CommentId)
		}
	}
	if len(pageSet) == 0 && len(authorSet) == 0 {
		return b, nil
	}
	rows, err := w.DB.Query(ctx, `SELECT subject_type, subject_key FROM suppression
		WHERE (subject_type = 'page' AND subject_key = ANY($1)) OR (subject_type = 'author' AND subject_key = ANY($2))`,
		keys(pageSet), keys(authorSet))
	if err != nil {
		return b, err
	}
	pages, authors := map[int64]bool{}, map[string]bool{}
	for rows.Next() {
		var typ, k string
		if err := rows.Scan(&typ, &k); err != nil {
			rows.Close()
			return b, err
		}
		if typ == "page" {
			id, _ := strconv.ParseInt(k, 10, 64)
			pages[id] = true
		} else {
			authors[k] = true
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return b, err
	}
	// Media of comments redacted earlier (an author erasure) or in this
	// batch is dropped with them.
	redacted := map[int64]bool{}
	if len(mediaComments) > 0 {
		rows, err := w.DB.Query(ctx, `SELECT comment_id FROM comments WHERE redacted AND comment_id = ANY($1)`, mediaComments)
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
	if len(pages) == 0 && len(authors) == 0 && len(redacted) == 0 {
		return b, nil
	}
	out := Batch{}
	drop := func(entity string) { metrics.PersistSuppressed.WithLabelValues(entity, "dropped").Inc() }
	for _, p := range b.Pages {
		if pages[p.PageId] {
			drop("page")
			continue
		}
		out.Pages = append(out.Pages, p)
	}
	for _, p := range b.Posts {
		if pages[p.PageId] {
			drop("post")
			continue
		}
		out.Posts = append(out.Posts, p)
	}
	for _, c := range b.Comments {
		if pages[c.PageId] {
			drop("comment")
			continue
		}
		if c.Author != nil && authors[hex.EncodeToString(c.Author.AuthorKey)] && !c.Redacted {
			redact(c)
			metrics.PersistSuppressed.WithLabelValues("comment", "redacted").Inc()
		}
		if c.Redacted {
			redacted[c.CommentId] = true
		}
		out.Comments = append(out.Comments, c)
	}
	for _, m := range b.Media {
		if pages[m.PageId] || (m.CommentId != nil && redacted[*m.CommentId]) {
			drop("media")
			continue
		}
		out.Media = append(out.Media, m)
	}
	return out, nil
}

// redact blanks a suppressed author's comment as the eraser does: the keys
// and times stay, the text, links, attachments and author fields go.
func redact(c *fbv1.Comment) {
	c.Author = &fbv1.Author{AuthorKey: c.Author.AuthorKey}
	c.Text, c.TextRanges, c.Attachments, c.Url, c.ExtraJson = "", nil, nil, "", ""
	c.Redacted = true
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// recordError reports whether err comes from a record's own data, so
// retrying the same batch can never succeed:
//   - a Postgres data exception (class 22, such as an invalid UUID or a
//     malformed UTF-8 sequence) or integrity violation (class 23);
//   - a value pgx cannot encode for its column (an empty or malformed
//     UUID): in a COPY, pgx aborts it and Postgres answers 57014 with pgx's
//     message; as a statement argument, pgx fails before sending.
//
// Anything else (the database down, a timeout, a deadlock, a bad
// statement) affects every record alike, and the batch is retried whole.
func recordError(err error) bool {
	return errorReason(err) != ""
}

// errorReason names a record error for the quarantine's reason header and
// metric label, or returns "" for any other error.
func errorReason(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		switch {
		case strings.HasPrefix(pe.Code, "22"), strings.HasPrefix(pe.Code, "23"):
			return "sqlstate_" + pe.Code
		case pe.Code == "57014" && strings.HasPrefix(pe.Message, "COPY from stdin failed: unable to encode"):
			return "encode"
		}
		return ""
	}
	if strings.Contains(err.Error(), "failed to encode args") {
		return "encode"
	}
	return ""
}
