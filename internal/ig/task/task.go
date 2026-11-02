// Package task builds and identifies igs's crawl tasks (the
// securebrand.igs.task.v1 envelope), as internal/crawl/task does for fbs's.
package task

import (
	"crypto/sha256"
	"encoding/base32"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/common/v1"
	igv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/ig/v1"
	igstaskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/igs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
)

var b32 = base32.StdEncoding.WithPadding(base32.NoPadding)

func hashID(parts ...string) string {
	h := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return strings.ToLower(b32.EncodeToString(h[:20]))
}

// ScopeKey identifies the stream a task paginates; used for leases.
func ScopeKey(t *igstaskv1.Task) string {
	switch s := t.Scope.(type) {
	case *igstaskv1.Task_Profile:
		if s.Profile.UserPk != 0 {
			return "profile:" + strconv.FormatInt(s.Profile.UserPk, 10)
		}
		return "profileref:" + s.Profile.ProfileRef
	case *igstaskv1.Task_Post:
		return "post:" + strconv.FormatInt(s.Post.MediaPk, 10)
	case *igstaskv1.Task_Media:
		return "media:" + s.Media.GetMedia().GetMediaId()
	}
	return "unknown"
}

// Finalize sets the deterministic task id (and lineage for a new stream).
func Finalize(t *igstaskv1.Task) *igstaskv1.Task {
	if t.LineageId == "" {
		t.LineageId = hashID("lineage", t.JobId, t.Kind.String(), ScopeKey(t), t.Mode.String())
	}
	t.TaskId = hashID(t.JobId, t.Kind.String(), ScopeKey(t), t.Mode.String(), t.LineageId, strconv.FormatUint(uint64(t.Seq), 10))
	if t.EnqueuedAt == nil {
		t.EnqueuedAt = timestamppb.Now()
	}
	return t
}

// Continuation returns the next segment of the same stream. Callers add
// what the segment saw to ItemsSeen.
func Continuation(t *igstaskv1.Task, cursor string) *igstaskv1.Task {
	c := clone(t)
	c.Seq = t.Seq + 1
	c.Cursor = cursor
	c.Attempt, c.Stalls = 0, 0
	c.EnqueuedAt = nil
	return Finalize(c)
}

// Retry returns the same task (same id) with attempt incremented.
func Retry(t *igstaskv1.Task, cursor string) *igstaskv1.Task {
	c := clone(t)
	c.Attempt = t.Attempt + 1
	if cursor != "" {
		c.Cursor = cursor
	}
	return c
}

// Replay returns a dead-lettered task ready to run again under a new id
// (see internal/crawl/task.Replay).
func Replay(t *igstaskv1.Task, now time.Time) *igstaskv1.Task {
	c := clone(t)
	c.TaskId = hashID("replay", t.TaskId, strconv.FormatInt(now.UnixNano(), 10))
	c.Attempt, c.Stalls = 0, 0
	c.EnqueuedAt = timestamppb.New(now)
	return c
}

func clone(t *igstaskv1.Task) *igstaskv1.Task { return proto.Clone(t).(*igstaskv1.Task) }

// NewProfile creates the root task of a profile job.
func NewProfile(jobID string, lane igstaskv1.Lane, mode commonv1.CrawlMode, ref string, userPK int64, lim *igstaskv1.Limits, sc *igstaskv1.ProfileScope) *igstaskv1.Task {
	if sc == nil {
		sc = &igstaskv1.ProfileScope{}
	}
	sc.ProfileRef, sc.UserPk = ref, userPK
	if u, ok := strings.CutPrefix(ref, "u:"); ok && sc.Username == "" {
		sc.Username = u
	}
	return Finalize(&igstaskv1.Task{
		JobId: jobID, Lane: lane, Kind: igstaskv1.Kind_KIND_PROFILE, Mode: mode, Limits: lim,
		Scope: &igstaskv1.Task_Profile{Profile: sc},
	})
}

// NewPost creates a post task (its detail, then its comments).
func NewPost(parent *igstaskv1.Task, mode commonv1.CrawlMode, s *igstaskv1.PostScope) *igstaskv1.Task {
	return Finalize(&igstaskv1.Task{
		JobId: parent.JobId, Lane: parent.Lane, Kind: igstaskv1.Kind_KIND_POST, Mode: mode, Limits: parent.Limits,
		Scope: &igstaskv1.Task_Post{Post: s}, Trace: parent.Trace,
	})
}

// NewMedia creates a download task. Media tasks are not part of job
// accounting.
func NewMedia(jobID string, m *igv1.Media) *igstaskv1.Task {
	return Finalize(&igstaskv1.Task{
		JobId: jobID, Kind: igstaskv1.Kind_KIND_MEDIA, Lane: igstaskv1.Lane_LANE_P1,
		Scope: &igstaskv1.Task_Media{Media: &igstaskv1.MediaScope{Media: m}},
	})
}

var (
	usernameRe = regexp.MustCompile(`^[A-Za-z0-9._]{1,30}$`)
	reserved   = map[string]bool{"p": true, "reel": true, "reels": true, "tv": true, "explore": true, "accounts": true,
		"stories": true, "direct": true, "about": true, "legal": true, "developer": true}
)

// NormalizeRef turns a profile reference into "u:<username>" or
// "id:<pk>": a username, @username, a profile URL, or id:<pk>.
func NormalizeRef(s string) (string, error) {
	s = strings.TrimSpace(s)
	if pk, ok := strings.CutPrefix(s, "id:"); ok {
		if n, err := strconv.ParseInt(pk, 10, 64); err == nil && n > 0 {
			return "id:" + pk, nil
		}
		return "", fmt.Errorf("task: %q is not a user pk", s)
	}
	if strings.Contains(s, "instagram.com") {
		if !strings.Contains(s, "://") {
			s = "https://" + s
		}
		u, err := url.Parse(s)
		if err != nil {
			return "", fmt.Errorf("task: %q: %w", s, err)
		}
		seg := strings.Split(strings.Trim(u.Path, "/"), "/")
		if len(seg) == 0 || seg[0] == "" || reserved[strings.ToLower(seg[0])] {
			return "", fmt.Errorf("task: %q is not a profile URL", s)
		}
		s = seg[0]
	}
	s = strings.TrimPrefix(s, "@")
	if !usernameRe.MatchString(s) {
		return "", fmt.Errorf("task: %q is not an Instagram username", s)
	}
	return "u:" + strings.ToLower(s), nil
}

// DefaultLimits are a job's limits from the ig.limits config.
func DefaultLimits(l config.IGLimits) *igstaskv1.Limits {
	vp := map[string]igstaskv1.VideoPolicy{"none": igstaskv1.VideoPolicy_VIDEO_POLICY_NONE, "thumbnail": igstaskv1.VideoPolicy_VIDEO_POLICY_THUMBNAIL,
		"sd": igstaskv1.VideoPolicy_VIDEO_POLICY_SD, "hd": igstaskv1.VideoPolicy_VIDEO_POLICY_HD}[l.VideoPolicy]
	return &igstaskv1.Limits{
		MaxAgeSeconds: int64(l.MaxAgeDays) * 86400, MaxPosts: l.MaxPosts, MaxCommentsPerPost: l.MaxCommentsPerPost,
		IncludeComments: l.IncludeComments, IncludeMedia: l.IncludeMedia, VideoPolicy: vp,
		IncludeReels: l.IncludeReels, IncludeHighlights: l.IncludeHighlights,
	}
}
