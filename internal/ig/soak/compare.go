package soak

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakeig"
)

// Compare returns how igs's store (schema ig) differs from what a complete
// read of the profile should have stored, as the e2e suite checks it: the
// profile; every post with its counts and pinned flag (a walled post
// unavailable, login_wall); every top-level comment with its text and
// reply count; and no visible post fakeig does not have.
func Compare(ctx context.Context, db *pgxpool.Pool, t fakeig.ProfileTruth) ([]string, error) {
	var diffs []string
	add := func(format string, args ...any) { diffs = append(diffs, fmt.Sprintf(t.Username+": "+format, args...)) }

	var username, fullName string
	var followers *int64
	var verified *bool
	err := db.QueryRow(ctx, `SELECT username, coalesce(full_name, ''), follower_count, verified FROM profiles WHERE user_pk = $1`,
		t.PK).Scan(&username, &fullName, &followers, &verified)
	if err != nil {
		add("profile not stored (%v)", err)
		return diffs, nil
	}
	if username != t.Username || fullName != t.FullName || followers == nil || *followers != t.Followers ||
		verified == nil || *verified != t.Verified {
		add("profile %q %q followers %v verified %v", username, fullName, show(followers), show(verified))
	}

	type row struct {
		status, reason         string
		likes, comments, plays *int64
		pinned                 *bool
	}
	stored := map[int64]row{}
	rows, err := db.Query(ctx, `SELECT media_pk, status, coalesce(status_reason, ''), like_count, comment_count, play_count,
		is_pinned FROM posts WHERE owner_pk = $1 AND kind <> 'highlight'`, t.PK)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var pk int64
		var r row
		if err := rows.Scan(&pk, &r.status, &r.reason, &r.likes, &r.comments, &r.plays, &r.pinned); err != nil {
			rows.Close()
			return nil, err
		}
		stored[pk] = r
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	known := map[int64]bool{}
	for _, pt := range t.Posts {
		known[pt.PK] = true
		r, ok := stored[pt.PK]
		switch {
		case !ok:
			add("post %d not stored", pt.PK)
			continue
		case pt.Gated:
			if r.status != "unavailable" || r.reason != "login_wall" {
				add("walled post %d: %s (%s), want unavailable (login_wall)", pt.PK, r.status, r.reason)
			}
			continue
		}
		if r.status != "visible" || !eq(r.likes, pt.Likes) || r.comments == nil || *r.comments != pt.CommentCount ||
			!eq(r.plays, pt.Plays) || r.pinned == nil || *r.pinned != pt.Pinned {
			add("post %d: %s, likes %v/%v, comments %v/%d, views %v/%v, pinned %v/%v", pt.PK, r.status, show(r.likes),
				show(pt.Likes), show(r.comments), pt.CommentCount, show(r.plays), show(pt.Plays), show(r.pinned), pt.Pinned)
		}
		d, err := compareComments(ctx, db, pt)
		if err != nil {
			return nil, err
		}
		for _, x := range d {
			add("post %d: %s", pt.PK, x)
		}
	}
	for pk, r := range stored {
		if !known[pk] && r.status == "visible" {
			add("post %d stored and visible, but not on fakeig", pk)
		}
	}
	return diffs, nil
}

func compareComments(ctx context.Context, db *pgxpool.Pool, pt fakeig.PostTruth) ([]string, error) {
	rows, err := db.Query(ctx, `SELECT comment_pk, coalesce(text, ''), coalesce(child_comment_count, 0) FROM comments
		WHERE media_pk = $1 AND depth = 0 AND status = 'visible'`, pt.PK)
	if err != nil {
		return nil, err
	}
	type c struct {
		text    string
		replies int64
	}
	got := map[int64]c{}
	for rows.Next() {
		var pk, replies int64
		var text string
		if err := rows.Scan(&pk, &text, &replies); err != nil {
			rows.Close()
			return nil, err
		}
		got[pk] = c{text, replies}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var diffs []string
	if len(got) != len(pt.Comments) {
		diffs = append(diffs, fmt.Sprintf("%d comments stored, want %d", len(got), len(pt.Comments)))
	}
	for _, w := range pt.Comments {
		if g, ok := got[w.PK]; !ok || g.text != w.Text || g.replies != int64(w.Replies) {
			diffs = append(diffs, fmt.Sprintf("comment %d: stored %v, text or replies (%d) differ", w.PK, ok, w.Replies))
			break
		}
	}
	return diffs, nil
}

func eq(a, b *int64) bool { return (a == nil && b == nil) || (a != nil && b != nil && *a == *b) }

func show[T any](p *T) any {
	if p == nil {
		return "none"
	}
	return *p
}
