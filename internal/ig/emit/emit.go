// Package emit publishes igs's entities (securebrand.ig.v1) to the ig.*.v1
// topics: it stamps each message's Observation (content hash and a stable
// event id), and applies the privacy rules to people (ADR 0005 §6):
// author keys, the pseudonymized mode and suppression.
package emit

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/zeebo/xxh3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/common/v1"
	igv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/ig/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/gdpr"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/topics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
)

// AuthorKey is the stable pseudonymous key of an Instagram user:
// HMAC-SHA256(key, "ig:user:" + pk), domain-separated from fbs's (ADR 0005
// §1). Usernames change; the pk does not.
func AuthorKey(key []byte, pk int64) []byte {
	if pk == 0 {
		return nil
	}
	m := hmac.New(sha256.New, key)
	m.Write([]byte("ig:user:" + strconv.FormatInt(pk, 10)))
	return m.Sum(nil)
}

// Source describes how entities were obtained.
type Source struct {
	JobID         string
	Query         string
	DocID         string
	ParserVersion uint32
	Mode          commonv1.CrawlMode
}

// Emitter publishes entities.
type Emitter struct {
	HMACKey      []byte
	Pseudonymize bool
	Suppression  *gdpr.Suppression
	Now          func() time.Time
}

func (e *Emitter) now() time.Time {
	if e.Now != nil {
		return e.Now()
	}
	return time.Now().UTC()
}

var marshal = proto.MarshalOptions{Deterministic: true}

// stamp computes the content hash over msg without its observation and
// volatile CDN signatures, and sets a fresh Observation.
func (e *Emitter) stamp(src Source, entityKey string, msg proto.Message, set func(*commonv1.Observation)) {
	set(nil)
	b, _ := marshal.Marshal(hashable(msg))
	h := xxh3.Hash128(b).Bytes()
	ev := xxh3.Hash128(append([]byte(entityKey+"|"), h[:]...)).Bytes()
	var jobs []string
	if src.JobID != "" {
		jobs = []string{src.JobID}
	}
	set(&commonv1.Observation{
		EventId: hex.EncodeToString(ev[:]), ObservedAt: timestamppb.New(e.now()), JobIds: jobs,
		QueryName: src.Query, DocId: src.DocID, ParserVersion: src.ParserVersion, ContentHash: h[:], Mode: src.Mode,
	})
}

func key(n int64) string { return strconv.FormatInt(n, 10) }

// Profile publishes a profile (keyed by user pk) with its author key.
func (e *Emitter) Profile(ctx context.Context, b *kafka.Batch, src Source, p *igv1.Profile) error {
	p.AuthorKey = AuthorKey(e.HMACKey, p.UserPk)
	e.stamp(src, "profile:"+key(p.UserPk), p, func(o *commonv1.Observation) { p.Obs = o })
	return b.AddProto(ctx, topics.Profile, key(p.UserPk), p)
}

// Post publishes a post's detail and a metrics sample (keyed by media pk).
func (e *Emitter) Post(ctx context.Context, b *kafka.Batch, src Source, p *igv1.Post) error {
	e.stamp(src, "post:"+key(p.MediaPk), p, func(o *commonv1.Observation) { p.Obs = o })
	if err := b.AddProto(ctx, topics.Post, key(p.MediaPk), p); err != nil {
		return err
	}
	return e.Metrics(ctx, b, src, &igv1.PostMetrics{MediaPk: p.MediaPk, OwnerPk: p.OwnerPk, TakenAt: p.TakenAt,
		LikeCount: p.LikeCount, CommentCount: p.CommentCount, PlayCount: p.PlayCount, CountsHidden: p.CountsHidden})
}

// Metrics publishes an engagement sample (the persister stores it only
// when the counts changed).
func (e *Emitter) Metrics(ctx context.Context, b *kafka.Batch, src Source, m *igv1.PostMetrics) error {
	m.CapturedAt = timestamppb.New(e.now())
	e.stamp(src, "post_metrics:"+key(m.MediaPk), m, func(o *commonv1.Observation) { m.Obs = o })
	return b.AddProto(ctx, topics.PostMetrics, key(m.MediaPk), m)
}

// Comment applies the privacy rules and publishes a comment (keyed by its
// post). It returns false when the comment was redacted.
func (e *Emitter) Comment(ctx context.Context, b *kafka.Batch, src Source, c *igv1.Comment) (bool, error) {
	keep := e.Privatize(c)
	e.stamp(src, "comment:"+key(c.CommentPk), c, func(o *commonv1.Observation) { c.Obs = o })
	return keep, b.AddProto(ctx, topics.Comment, key(c.MediaPk), c)
}

// Privatize sets the author key, applies the pseudonymized mode and
// redacts a suppressed author's comment. It returns false if redacted.
func (e *Emitter) Privatize(c *igv1.Comment) bool {
	if c.Author == nil {
		return true
	}
	c.Author.AuthorKey = AuthorKey(e.HMACKey, c.Author.UserPk)
	if e.Suppression.AuthorSuppressed(c.Author.AuthorKey) {
		c.Author = &igv1.Author{AuthorKey: c.Author.AuthorKey}
		c.Text = ""
		c.Redacted = true
		return false
	}
	if e.Pseudonymize {
		c.Author = &igv1.Author{AuthorKey: c.Author.AuthorKey, IsVerified: c.Author.IsVerified}
	}
	return true
}

// Media publishes discovered media metadata (keyed by media id).
func (e *Emitter) Media(ctx context.Context, b *kafka.Batch, src Source, m *igv1.Media) error {
	e.stamp(src, "media:"+m.MediaId, m, func(o *commonv1.Observation) { m.Obs = o })
	return b.AddProto(ctx, topics.Media, m.MediaId, m)
}

// Highlight publishes a highlight (keyed by its owner).
func (e *Emitter) Highlight(ctx context.Context, b *kafka.Batch, src Source, h *igv1.Highlight) error {
	e.stamp(src, "highlight:"+key(h.HighlightId), h, func(o *commonv1.Observation) { h.Obs = o })
	return b.AddProto(ctx, topics.Highlight, key(h.UserPk), h)
}

// hashable returns a copy of msg without volatile values: signed CDN URLs
// carry per-request expiry and signature parameters, and source expiries
// move with them, so they would change the hash on every scrape.
func hashable(msg proto.Message) proto.Message {
	c := proto.Clone(msg)
	strip(c.ProtoReflect())
	return c
}

func strip(m protoreflect.Message) {
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.Name() == "source_expires_at":
			m.Clear(fd)
		case fd.IsList():
			l := v.List()
			for i := 0; i < l.Len(); i++ {
				if fd.Kind() == protoreflect.MessageKind {
					strip(l.Get(i).Message())
				} else if fd.Kind() == protoreflect.StringKind {
					l.Set(i, protoreflect.ValueOfString(stableURL(l.Get(i).String())))
				}
			}
		case fd.IsMap():
		case fd.Kind() == protoreflect.MessageKind:
			strip(v.Message())
		case fd.Kind() == protoreflect.StringKind:
			if s := v.String(); strings.Contains(s, "oe=") || strings.Contains(s, "_nc_") {
				m.Set(fd, protoreflect.ValueOfString(stableURL(s)))
			}
		}
		return true
	})
}

func stableURL(s string) string {
	if !strings.Contains(s, "?") {
		return s
	}
	u, err := url.Parse(s)
	if err != nil {
		return s
	}
	q := u.Query()
	if q.Get("oe") == "" && !strings.Contains(u.RawQuery, "_nc_") {
		return s
	}
	u.RawQuery = ""
	return u.String()
}
