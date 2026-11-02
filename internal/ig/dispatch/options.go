package dispatch

import (
	"errors"

	igstaskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/igs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/task"
)

// CrawlOptions are the crawl settings a job or a registered profile may
// set over the ig.limits defaults (the API's JSON, stored on schedules).
type CrawlOptions struct {
	MaxAgeDays         *int    `json:"max_age_days,omitempty"`
	MaxPosts           *int64  `json:"max_posts,omitempty"`
	MaxCommentsPerPost *int64  `json:"max_comments_per_post,omitempty"`
	IncludeComments    *bool   `json:"include_comments,omitempty"`
	IncludeReels       *bool   `json:"include_reels,omitempty"`
	IncludeHighlights  *bool   `json:"include_highlights,omitempty"`
	IncludeMedia       *bool   `json:"include_media,omitempty"`
	VideoPolicy        *string `json:"video_policy,omitempty"`
}

// Limits applies o over cfg's ig.limits, enforcing the age cap.
func Limits(cfg *config.Config, o CrawlOptions) (*igstaskv1.Limits, error) {
	l := task.DefaultLimits(cfg.IG.Limits)
	if o.MaxAgeDays != nil {
		if *o.MaxAgeDays <= 0 || *o.MaxAgeDays > cfg.IG.Limits.MaxAgeCapDays {
			return nil, errors.New("max_age_days must be between 1 and the configured cap")
		}
		l.MaxAgeSeconds = int64(*o.MaxAgeDays) * 86400
	}
	if o.MaxPosts != nil {
		if *o.MaxPosts < 0 {
			return nil, errors.New("max_posts must not be negative")
		}
		l.MaxPosts = *o.MaxPosts
	}
	if o.MaxCommentsPerPost != nil {
		if *o.MaxCommentsPerPost < 0 {
			return nil, errors.New("max_comments_per_post must not be negative")
		}
		l.MaxCommentsPerPost = *o.MaxCommentsPerPost
	}
	set := func(dst *bool, v *bool) {
		if v != nil {
			*dst = *v
		}
	}
	set(&l.IncludeComments, o.IncludeComments)
	set(&l.IncludeReels, o.IncludeReels)
	set(&l.IncludeHighlights, o.IncludeHighlights)
	set(&l.IncludeMedia, o.IncludeMedia)
	if o.VideoPolicy != nil {
		vp, ok := map[string]igstaskv1.VideoPolicy{"none": igstaskv1.VideoPolicy_VIDEO_POLICY_NONE,
			"thumbnail": igstaskv1.VideoPolicy_VIDEO_POLICY_THUMBNAIL, "sd": igstaskv1.VideoPolicy_VIDEO_POLICY_SD,
			"hd": igstaskv1.VideoPolicy_VIDEO_POLICY_HD}[*o.VideoPolicy]
		if !ok {
			return nil, errors.New("video_policy must be none, thumbnail, sd or hd")
		}
		l.VideoPolicy = vp
	}
	return l, nil
}
