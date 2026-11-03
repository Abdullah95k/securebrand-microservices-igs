package discovery

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/task"
)

// MaxIntake is the most accounts one intake call takes.
const MaxIntake = 1000

var originRe = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,31}$`)

// IntakeResult says what an intake did.
type IntakeResult struct {
	Added   int      `json:"added"`   // new candidates
	Known   int      `json:"known"`   // already candidates: their origin is added
	Invalid []string `json:"invalid"` // not usernames or profile links
}

// Intake adds accounts found elsewhere (the Google discovery tool, links
// on Facebook pages and YouTube channels, a list by hand) as candidates,
// vetted and scored like the ones the crawl meets. Each entry is a username
// or a profile link; origin says where they came from (google, facebook,
// youtube, manual...). They are queued ahead of single sightings
// (min_priority + 10). Known candidates keep their status: an intake is no
// new evidence about an account already decided.
func Intake(ctx context.Context, db *pgxpool.Pool, cfg config.IGDiscovery, entries []string, origin string) (IntakeResult, error) {
	var res IntakeResult
	origin = strings.ToLower(strings.TrimSpace(origin))
	if !originRe.MatchString(origin) || origin == "network" {
		return res, fmt.Errorf("discovery: origin %q must be a short lower-case name other than network", origin)
	}
	if len(entries) > MaxIntake {
		return res, fmt.Errorf("discovery: at most %d accounts a call, got %d", MaxIntake, len(entries))
	}
	seen := map[string]bool{}
	var users []string
	for _, e := range entries {
		ref, err := task.NormalizeRef(strings.TrimPrefix(strings.TrimSpace(e), "@"))
		u, ok := strings.CutPrefix(ref, "u:")
		u = strings.ToLower(u)
		if err != nil || !ok || !usernameRe.MatchString(u) {
			if strings.TrimSpace(e) != "" {
				res.Invalid = append(res.Invalid, e)
			}
			continue
		}
		if !seen[u] {
			seen[u] = true
			users = append(users, u)
		}
	}
	if len(users) == 0 {
		return res, nil
	}
	rows, err := db.Query(ctx, `
		INSERT INTO discovery_candidates AS c (username, origins, priority)
		SELECT u, ARRAY[$2::text], $3 FROM unnest($1::text[]) AS u
		ON CONFLICT (username) DO UPDATE SET
			origins = CASE WHEN $2 = ANY(c.origins) THEN c.origins ELSE c.origins || ARRAY[$2::text] END,
			priority = greatest(c.priority, EXCLUDED.priority), last_seen_at = now()
		RETURNING xmax = 0`, users, origin, cfg.MinPriority+10)
	if err != nil {
		return res, err
	}
	defer rows.Close()
	for rows.Next() {
		var added bool
		if err := rows.Scan(&added); err != nil {
			return res, err
		}
		if added {
			res.Added++
		} else {
			res.Known++
		}
	}
	return res, rows.Err()
}
