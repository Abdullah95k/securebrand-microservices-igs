package igdisc

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// EraseProfile deletes, in igs's erasure transaction, the evidence about
// an erased account (by user pk, or by the username the account or its
// candidate had) and the evidence found on its posts. Run it before igs
// empties the profile's row and deletes its candidate, which hold the
// username.
func EraseProfile(ctx context.Context, tx pgx.Tx, pk int64) (int64, error) {
	tag, err := tx.Exec(ctx, `DELETE FROM igdisc_evidence WHERE source_pk = $1 OR user_pk = $1
		OR username IN (SELECT lower(username) FROM profiles WHERE user_pk = $1 AND username IS NOT NULL
			UNION SELECT username FROM discovery_candidates WHERE user_pk = $1)`, pk)
	return tag.RowsAffected(), err
}

// EraseAuthor deletes, in igs's erasure transaction, the evidence about an
// erased comment author (by author key). Run it before igs deletes the
// author's comment_authors row, which links the key to the user pk.
func EraseAuthor(ctx context.Context, tx pgx.Tx, key []byte) (int64, error) {
	tag, err := tx.Exec(ctx, `DELETE FROM igdisc_evidence WHERE user_pk IN
		(SELECT author_pk FROM comment_authors WHERE author_key = $1 AND author_pk IS NOT NULL)
		OR username IN (SELECT username FROM discovery_candidates WHERE user_pk IN
			(SELECT author_pk FROM comment_authors WHERE author_key = $1 AND author_pk IS NOT NULL))`, key)
	return tag.RowsAffected(), err
}
