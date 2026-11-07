// Package health gathers what an operator checks first about igs
// (REQ-008 fix 19, F5): read-only counts from the store, Valkey and Kafka,
// each with whether it looks right and what to do when it doesn't.
// igsctl health prints them; docs/runbooks/igs-operations.md, "Health",
// explains each line.
package health

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/iraqtime"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/ratelimit"
)

// Status is how a check looks.
type Status int

const (
	OK Status = iota
	Warn
	Bad
	Unknown // the source did not answer
)

func (s Status) String() string {
	return [...]string{"OK", "WARN", "BAD", "?"}[s]
}

// Check is one line of the report.
type Check struct {
	Name   string
	Status Status
	Value  string
	// Do says what to look at when Status is not OK.
	Do string
}

// Sources are where the checks read. DB is required (schema ig); Redis and
// Admin may be nil, and their checks are then left out.
type Sources struct {
	DB     *pgxpool.Pool
	Redis  *redis.Client
	Admin  *kadm.Client
	Cfg    *config.Config
	Now    func() time.Time
	Prefix string // kafka.topic_prefix: consumer groups and Valkey keys
}

// NoDataAfter is how long without a profile read is wrong while profiles
// are on the schedule: one full long-tail interval (IgsIgNoData).
const NoDataAfter = 3 * time.Hour

// LagWarn is the consumer lag past which a group is reported.
const LagWarn = 10_000

// Gather runs every check. It never fails: a source that does not answer
// gives an Unknown check naming its error.
func Gather(ctx context.Context, s Sources) []Check {
	if s.Now == nil {
		s.Now = time.Now
	}
	checks := []Check{profiles(ctx, s), schedule(ctx, s), jobs(ctx, s), lastData(ctx, s)}
	if s.Redis != nil {
		checks = append(checks, exits(ctx, s))
	}
	if s.Admin != nil {
		checks = append(checks, lag(ctx, s))
	}
	return checks
}

func unknown(name string, err error) Check {
	return Check{Name: name, Status: Unknown, Value: "could not read: " + err.Error()}
}

func profiles(ctx context.Context, s Sources) Check {
	const name = "profiles on the schedule"
	rows, err := s.DB.Query(ctx, `SELECT tier, count(*) FILTER (WHERE paused_until IS NULL OR paused_until <= now()),
		count(*) FILTER (WHERE paused_until > now()) FROM profile_schedule WHERE enabled GROUP BY tier ORDER BY tier`)
	if err != nil {
		return unknown(name, err)
	}
	defer rows.Close()
	var parts []string
	var active, paused int64
	for rows.Next() {
		var tier string
		var a, p int64
		if err := rows.Scan(&tier, &a, &p); err != nil {
			return unknown(name, err)
		}
		active += a
		paused += p
		parts = append(parts, fmt.Sprintf("%s %d", strings.ReplaceAll(tier, "_", " "), a))
	}
	if err := rows.Err(); err != nil {
		return unknown(name, err)
	}
	c := Check{Name: name, Value: fmt.Sprintf("%d", active)}
	if len(parts) > 0 {
		c.Value += " (" + strings.Join(parts, ", ") + ")"
	}
	if paused > 0 {
		c.Value += fmt.Sprintf("; %d paused", paused)
	}
	if active == 0 {
		c.Status, c.Do = Warn, "nothing to collect: register profiles (POST /v1/profiles or igsctl profiles import)"
	}
	return c
}

func schedule(ctx context.Context, s Sources) Check {
	const name = "schedule"
	var overdue int64
	var oldest *time.Duration
	var secs *float64
	err := s.DB.QueryRow(ctx, `SELECT count(*), max(extract(epoch FROM now() - next_run_at))::float8 FROM profile_schedule
		WHERE enabled AND (paused_until IS NULL OR paused_until <= now())
			AND next_run_at < now() - interval_seconds * interval '1 second'`).Scan(&overdue, &secs)
	if err != nil {
		return unknown(name, err)
	}
	if secs != nil {
		d := time.Duration(*secs) * time.Second
		oldest = &d
	}
	if overdue == 0 {
		return Check{Name: name, Value: "on time: no profile overdue by its own interval"}
	}
	return Check{Name: name, Status: Bad, Value: fmt.Sprintf("%d profiles overdue by their own interval; the oldest by %s", overdue, round(*oldest)),
		Do: "crawling doesn't keep up: check the crawlers, the exits resting and the lag below (runbook: Schedule)"}
}

func jobs(ctx context.Context, s Sources) Check {
	const name = "jobs, last hour"
	rows, err := s.DB.Query(ctx, `SELECT status, count(*) FROM jobs
		WHERE created_at > now() - interval '1 hour' OR status IN ('queued', 'running', 'finalizing') GROUP BY status`)
	if err != nil {
		return unknown(name, err)
	}
	defer rows.Close()
	n := map[string]int64{}
	for rows.Next() {
		var st string
		var c int64
		if err := rows.Scan(&st, &c); err != nil {
			return unknown(name, err)
		}
		n[st] = c
	}
	if err := rows.Err(); err != nil {
		return unknown(name, err)
	}
	c := Check{Name: name, Value: fmt.Sprintf("%d completed, %d partial, %d failed; %d running, %d queued",
		n["completed"], n["partial"], n["failed"], n["running"]+n["finalizing"], n["queued"])}
	if done := n["completed"] + n["partial"] + n["failed"]; done > 0 && n["failed"]*5 > done {
		c.Status, c.Do = Warn, "more than a fifth failed: GET /v1/jobs/{id} for a failed job's reason (runbook: Stuck jobs)"
	}
	return c
}

func lastData(ctx context.Context, s Sources) Check {
	const name = "last data stored"
	var profile, post *time.Time
	var scheduled int64
	err := s.DB.QueryRow(ctx, `SELECT (SELECT max(last_profile_read_at) FROM profiles),
		(SELECT max(last_seen_at) FROM posts WHERE taken_at > now() - interval '400 days'),
		(SELECT count(*) FROM profile_schedule WHERE enabled)`).Scan(&profile, &post, &scheduled)
	if err != nil {
		return unknown(name, err)
	}
	loc := Iraq
	at := func(t *time.Time) string {
		if t == nil {
			return "never"
		}
		return t.In(loc).Format("2006-01-02 15:04")
	}
	c := Check{Name: name, Value: fmt.Sprintf("profile read %s, post seen %s (Iraq time)", at(profile), at(post))}
	if scheduled > 0 && (profile == nil || s.Now().Sub(*profile) > NoDataAfter) {
		c.Status, c.Do = Bad, "no profile read in 3 hours though profiles are on the schedule: crawling, publishing or storing has stopped (runbook: No data)"
	}
	return c
}

func exits(ctx context.Context, s Sources) Check {
	const name = "proxy exits resting"
	rests, err := ratelimit.NewExitRests(s.Redis, s.Prefix).Load(ctx)
	if err != nil {
		return unknown(name, err)
	}
	resting := map[string]int{}
	for exit, until := range rests {
		if until.After(s.Now()) {
			tier, _, _ := strings.Cut(exit, "#")
			resting[tier]++
		}
	}
	c := Check{Name: name}
	var parts []string
	for _, t := range s.Cfg.IG.Proxy.Tiers {
		if t.Kind != "static_list" {
			continue
		}
		n := len(t.Endpoints)
		parts = append(parts, fmt.Sprintf("%s %d of %d", t.Name, resting[t.Name], n))
		if n > 0 && resting[t.Name]*2 > n {
			c.Status, c.Do = Warn, "most of a tier's exits rest: Instagram is limiting them (runbook: Block storm)"
		}
	}
	if len(parts) == 0 {
		var total int
		for _, n := range resting {
			total += n
		}
		parts = append(parts, fmt.Sprintf("%d resting (no static tier in this config)", total))
	}
	c.Value = strings.Join(parts, ", ")
	return c
}

func lag(ctx context.Context, s Sources) Check {
	const name = "Kafka lag"
	listed, err := s.Admin.ListGroups(ctx)
	if err != nil {
		return unknown(name, err)
	}
	var groups []string
	for _, g := range listed.Groups() {
		if strings.HasPrefix(g, s.Prefix+"igs-") {
			groups = append(groups, g)
		}
	}
	if len(groups) == 0 {
		return Check{Name: name, Status: Warn, Value: "no igs consumer group yet", Do: "no consumer has joined: are the persister and crawlers running?"}
	}
	lags, err := s.Admin.Lag(ctx, groups...)
	if err != nil {
		return unknown(name, err)
	}
	type gl struct {
		group string
		lag   int64
	}
	var all []gl
	for _, l := range lags {
		if l.Error() != nil {
			continue
		}
		all = append(all, gl{strings.TrimPrefix(l.Group, s.Prefix), max(l.Lag.Total(), 0)})
	}
	slices.SortFunc(all, func(a, b gl) int { return int(b.lag - a.lag) })
	c := Check{Name: name}
	var parts []string
	for i, g := range all {
		if i < 5 {
			parts = append(parts, fmt.Sprintf("%s %d", strings.TrimPrefix(g.group, "igs-"), g.lag))
		}
		if g.lag > LagWarn {
			c.Status, c.Do = Warn, "a consumer falls behind: scale it or look at its errors (runbook: Kafka lag)"
		}
	}
	c.Value = strings.Join(parts, ", ")
	if len(all) > 5 {
		c.Value += fmt.Sprintf(" (%d more groups, less behind)", len(all)-5)
	}
	return c
}

// Iraq is the owner's time zone (UTC+3, no daylight saving), for the times
// the report shows.
var Iraq = iraqtime.Zone

func round(d time.Duration) string {
	if d >= time.Hour {
		return d.Round(time.Minute).String()
	}
	return d.Round(time.Second).String()
}

// Print writes the report: one line per check, then what to do for those
// not OK. It returns whether any check is Bad.
func Print(w io.Writer, checks []Check, now time.Time) bool {
	_, _ = fmt.Fprintf(w, "igs health at %s Iraq time\n", now.In(Iraq).Format("2006-01-02 15:04"))
	width := 0
	for _, c := range checks {
		width = max(width, len(c.Name))
	}
	bad := false
	for _, c := range checks {
		_, _ = fmt.Fprintf(w, "%-5s %-*s  %s\n", c.Status, width, c.Name, c.Value)
		bad = bad || c.Status == Bad
	}
	for _, c := range checks {
		if c.Status != OK && c.Do != "" {
			_, _ = fmt.Fprintf(w, "  %s: %s\n", c.Name, c.Do)
		}
	}
	return bad
}
