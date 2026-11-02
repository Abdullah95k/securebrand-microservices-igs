package persist

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/jackc/pgx/v5/pgconn"

	igv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/ig/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/topics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
)

// Group is igs's persister's consumer group; the job finalizer waits for
// its committed offsets.
const Group = "igs-ig-persister"

// Consumer reads entity topics in batches and writes them.
type Consumer struct {
	cl       *kgo.Client
	w        *Writer
	dlq      *kafka.Producer // quarantine; nil logs and skips (tests)
	prefix   string
	maxBatch int
	linger   time.Duration
}

// NewConsumer reads the entity topics into w. Records it cannot write go to
// the persister's DLQ through dlq.
func NewConsumer(cfg kafka.ClientConfig, w *Writer, dlq *kafka.Producer, maxBatch int, linger time.Duration) (*Consumer, error) {
	ts := make([]string, len(topics.Entity))
	for i, t := range topics.Entity {
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
		maxBatch = 2000
	}
	return &Consumer{cl: cl, w: w, dlq: dlq, prefix: cfg.TopicPrefix, maxBatch: maxBatch, linger: linger}, nil
}

// Run consumes until ctx is done. A batch is committed only after every
// record in it was written or quarantined.
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
					slog.Warn("persist: fetch error", "topic", t, "partition", p, "err", err)
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
		if err := c.flush(ctx, recs); err != nil {
			if ctx.Err() != nil {
				// Shutting down: the batch is not committed and is read again.
				c.cl.AllowRebalance()
				return nil
			}
			slog.Error("persist: batch failed; retrying", "records", len(recs), "err", err)
			// An error here affects the whole batch (the database is down,
			// a statement fails for every record): re-write it until it
			// succeeds, never committing past unwritten data. Records that
			// fail on their own data were quarantined by flush.
			for ctx.Err() == nil {
				time.Sleep(2 * time.Second)
				if err = c.flush(ctx, recs); err == nil {
					break
				}
				slog.Error("persist: batch retry failed", "err", err)
			}
			if ctx.Err() != nil {
				c.cl.AllowRebalance()
				return nil
			}
		}
		if err := c.cl.CommitRecords(ctx, recs...); err != nil {
			slog.Warn("persist: commit failed", "err", err)
		}
		c.cl.AllowRebalance()
	}
}

// item is a decoded record.
type item struct {
	rec *kgo.Record
	msg proto.Message
}

// bad is a record the persister cannot write.
type bad struct {
	rec            *kgo.Record
	reason, detail string
}

// flush writes recs. A record that does not decode, and one whose write
// fails on its own data (found by bisecting the batch, see isolate), is
// quarantined so it cannot stall the persister; an error means the batch
// as a whole must be retried. Before, an undecodable record was written
// half-decoded, and one bad record was retried forever.
func (c *Consumer) flush(ctx context.Context, recs []*kgo.Record) error {
	items, bads := c.decode(recs)
	isolated, err := isolate(ctx, items, c.write)
	if err != nil {
		return err
	}
	return c.quarantine(ctx, append(bads, isolated...))
}

func (c *Consumer) decode(recs []*kgo.Record) ([]item, []bad) {
	items := make([]item, 0, len(recs))
	var bads []bad
	for _, r := range recs {
		var m proto.Message
		switch strings.TrimPrefix(r.Topic, c.prefix) {
		case topics.Profile:
			m = &igv1.Profile{}
		case topics.Post:
			m = &igv1.Post{}
		case topics.PostMetrics:
			m = &igv1.PostMetrics{}
		case topics.Comment:
			m = &igv1.Comment{}
		case topics.Media:
			m = &igv1.Media{}
		case topics.Highlight:
			m = &igv1.Highlight{}
		default:
			continue
		}
		if err := proto.Unmarshal(r.Value, m); err != nil {
			bads = append(bads, bad{rec: r, reason: "undecodable", detail: err.Error()})
			continue
		}
		items = append(items, item{rec: r, msg: m})
	}
	return items, bads
}

func (c *Consumer) write(ctx context.Context, items []item) error {
	var b Batch
	for _, it := range items {
		switch m := it.msg.(type) {
		case *igv1.Profile:
			b.Profiles = append(b.Profiles, m)
		case *igv1.Post:
			b.Posts = append(b.Posts, m)
		case *igv1.PostMetrics:
			b.Metrics = append(b.Metrics, m)
		case *igv1.Comment:
			b.Comments = append(b.Comments, m)
		case *igv1.Media:
			b.Media = append(b.Media, m)
		case *igv1.Highlight:
			b.Highlights = append(b.Highlights, m)
		}
	}
	return c.w.Write(ctx, b)
}

// isolate writes items and, when the write fails on a record's data
// (recordError), bisects the batch until it has found every record that
// fails alone; the others are written (writes are idempotent upserts, so a
// half written twice changes nothing). Any other error is returned at once:
// it is not the records', and the batch is retried whole.
func isolate(ctx context.Context, items []item, write func(context.Context, []item) error) ([]bad, error) {
	if len(items) == 0 {
		return nil, nil
	}
	err := write(ctx, items)
	if err == nil {
		return nil, nil
	}
	if !recordError(err) {
		return nil, err
	}
	if len(items) == 1 {
		return []bad{{rec: items[0].rec, reason: errorReason(err), detail: err.Error()}}, nil
	}
	mid := len(items) / 2
	left, err := isolate(ctx, items[:mid], write)
	if err != nil {
		return nil, err
	}
	right, err := isolate(ctx, items[mid:], write)
	if err != nil {
		return nil, err
	}
	return append(left, right...), nil
}

// quarantine moves bad records to the persister's DLQ, with their origin
// (unprefixed topic, partition, offset), the reason and the error.
func (c *Consumer) quarantine(ctx context.Context, bads []bad) error {
	if len(bads) == 0 {
		return nil
	}
	out := make([]*kgo.Record, 0, len(bads))
	for _, b := range bads {
		topic := strings.TrimPrefix(b.rec.Topic, c.prefix)
		metrics.PersistQuarantined.WithLabelValues(topic, b.reason).Inc()
		slog.Error("persist: record quarantined", "topic", topic, "partition", b.rec.Partition, "offset", b.rec.Offset,
			"reason", b.reason, "err", b.detail)
		if c.dlq == nil {
			continue
		}
		out = append(out, &kgo.Record{Topic: c.dlq.Topic(topics.PersistDLQ), Key: b.rec.Key, Value: b.rec.Value, Headers: append(
			slices.Clone(b.rec.Headers),
			kgo.RecordHeader{Key: kafka.HeaderOrigTopic, Value: []byte(topic)},
			kgo.RecordHeader{Key: kafka.HeaderOrigPartition, Value: []byte(strconv.Itoa(int(b.rec.Partition)))},
			kgo.RecordHeader{Key: kafka.HeaderOrigOffset, Value: []byte(strconv.FormatInt(b.rec.Offset, 10))},
			kgo.RecordHeader{Key: kafka.HeaderReason, Value: []byte(b.reason)},
			kgo.RecordHeader{Key: kafka.HeaderError, Value: []byte(truncate(b.detail, 1000))})})
	}
	if len(out) == 0 {
		return nil
	}
	if err := c.dlq.ProduceSync(ctx, out...); err != nil {
		return fmt.Errorf("persist: quarantine: %w", err)
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// recordError reports whether err comes from a record's own data, so
// retrying the same batch can never succeed: a Postgres data exception
// (class 22) or integrity violation (class 23), or a value pgx cannot
// encode for its column (as fbs's persister, internal/persist).
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
			if pe.Code == "23514" && strings.Contains(pe.Message, "no partition") {
				return "" // a missing partition is created and retried
			}
			return "sqlstate_" + pe.Code
		case pe.Code == "57014" && strings.HasPrefix(pe.Message, "COPY from stdin failed: unable to encode"):
			return "encode"
		}
		return ""
	}
	if strings.Contains(err.Error(), "failed to encode args") || strings.Contains(err.Error(), "unable to encode") {
		return "encode"
	}
	return ""
}
