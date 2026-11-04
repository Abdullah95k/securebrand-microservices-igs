package clickhouse

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	contentv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/content/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/topics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
)

// Group is the writer's consumer group (base name).
const Group = "igs-content-writer"

// Consumer reads the content.* topics in batches into the store. A batch
// is committed only once every table's rows were inserted; a failed insert
// is retried until it succeeds, so nothing is skipped. Inserting a batch
// twice is harmless: rows of the same entity and version collapse.
type Consumer struct {
	cl       *kgo.Client
	ins      Inserter
	prefix   string
	maxBatch int
	linger   time.Duration
}

// NewConsumer reads every content topic into ins.
func NewConsumer(cfg kafka.ClientConfig, ins Inserter, maxBatch int, linger time.Duration) (*Consumer, error) {
	ts := make([]string, len(topics.Content))
	for i, t := range topics.Content {
		ts[i] = cfg.TopicPrefix + t
	}
	cl, err := kgo.NewClient(append(cfg.Opts(),
		kgo.ConsumerGroup(cfg.TopicPrefix+Group),
		kgo.ConsumeTopics(ts...),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)...)
	if err != nil {
		return nil, err
	}
	if maxBatch <= 0 {
		maxBatch = 5000
	}
	if linger <= 0 {
		linger = time.Second
	}
	return &Consumer{cl: cl, ins: ins, prefix: cfg.TopicPrefix, maxBatch: maxBatch, linger: linger}, nil
}

// Decode maps a content record (topic base name) to its message, or nil
// for a topic that is not a content topic.
func Decode(topic string, value []byte) (proto.Message, error) {
	var m proto.Message
	switch topic {
	case topics.ContentPost:
		m = &contentv1.Post{}
	case topics.ContentComment:
		m = &contentv1.Comment{}
	case topics.ContentAccount:
		m = &contentv1.Account{}
	case topics.ContentMetrics:
		m = &contentv1.Metrics{}
	case topics.ContentMedia:
		m = &contentv1.Media{}
	default:
		return nil, nil
	}
	return m, proto.Unmarshal(value, m)
}

// Run consumes until ctx is done.
func (c *Consumer) Run(ctx context.Context) error {
	defer c.cl.Close()
	for {
		var recs []*kgo.Record
		deadline := time.Now().Add(c.linger)
		for len(recs) < c.maxBatch {
			pctx, cancel := context.WithDeadline(ctx, deadline)
			f := c.cl.PollRecords(pctx, c.maxBatch-len(recs))
			cancel()
			if ctx.Err() != nil {
				c.cl.AllowRebalance()
				return nil
			}
			f.EachError(func(t string, p int32, err error) {
				if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
					slog.Warn("content writer: fetch error", "topic", t, "partition", p, "err", err)
				}
			})
			recs = append(recs, f.Records()...)
			if time.Now().After(deadline) || (len(recs) > 0 && f.NumRecords() == 0) {
				break
			}
		}
		if len(recs) == 0 {
			c.cl.AllowRebalance()
			continue
		}
		rows := Rows{}
		for _, r := range recs {
			base := strings.TrimPrefix(r.Topic, c.prefix)
			m, err := Decode(base, r.Value)
			if err != nil {
				metrics.ContentWritten.WithLabelValues(base, "undecodable").Inc()
				slog.Warn("content writer: record skipped", "topic", base, "partition", r.Partition, "offset", r.Offset, "err", err)
				continue
			}
			if m != nil {
				rows.Add(m)
			}
		}
		for err := Write(ctx, c.ins, rows); err != nil; err = Write(ctx, c.ins, rows) {
			if ctx.Err() != nil {
				c.cl.AllowRebalance()
				return nil
			}
			slog.Error("content writer: insert failed; retrying", "records", len(recs), "err", err)
			select {
			case <-ctx.Done():
			case <-time.After(2 * time.Second):
			}
		}
		for t, rs := range rows {
			metrics.ContentWritten.WithLabelValues(t, "ok").Add(float64(len(rs)))
		}
		if err := c.cl.CommitRecords(ctx, recs...); err != nil {
			slog.Warn("content writer: commit failed", "err", err)
		}
		c.cl.AllowRebalance()
	}
}
