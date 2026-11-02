package topics

import (
	"context"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"os"
	"sort"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"

	protosrc "github.com/abdullah95k/securebrand-microservices-igs/api/proto"
	"github.com/abdullah95k/securebrand-microservices-igs/configs"
	igstaskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/igs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/schemareg"
)

func applied(t *testing.T) map[string]bool {
	t.Helper()
	b, err := configs.FS.ReadFile("ig/topics.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Topics []kafka.TopicSpec `yaml:"topics"`
	}
	if err := yaml.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for _, s := range doc.Topics {
		out[s.Name] = true
	}
	return out
}

// configs/ig/topics.yaml creates exactly the topics igs uses, and none of
// them is one of fbs's (ADR 0004 §4: shared infrastructure).
func TestTopicsMatchTheirConfig(t *testing.T) {
	want := map[string]bool{TaskMedia: true, Retry30s: true, Retry5m: true, DLQ: true, PersistDLQ: true}
	for t := range ValueSchemas {
		want[t] = true
	}
	for _, t := range TaskTopics() {
		want[t] = true
	}
	got := applied(t)
	for n := range want {
		if !got[n] {
			t.Errorf("%s is used but not in configs/ig/topics.yaml", n)
		}
	}
	for n := range got {
		if !want[n] {
			t.Errorf("configs/ig/topics.yaml creates %s, which igs does not use", n)
		}
		// fbs's topics are fb.*.v1 and fbs.task.* (ADR 0004 §4).
		if strings.HasPrefix(n, "fb.") || strings.HasPrefix(n, "fbs.") {
			t.Errorf("%s is one of fbs's topic names", n)
		}
	}
	for _, e := range Entity {
		if _, ok := ValueSchemas[e]; !ok {
			t.Errorf("entity topic %s has no schema", e)
		}
	}
}

// Every public topic's schema file exists in the embedded contract.
func TestValueSchemasExist(t *testing.T) {
	for topic, file := range ValueSchemas {
		if _, err := fs.Stat(protosrc.FS, file); err != nil {
			t.Errorf("%s: %v", topic, err)
		}
	}
}

// fbs's wire format of its task envelope, securebrand.fbs.task.v1.Task,
// which shares Kafka with igs's (D6) but is no longer in this repository:
// field numbers and kind values as fbs's api/proto/securebrand/fbs/task/v1
// declares them.
const (
	fbsTaskID    = 1
	fbsJobID     = 4
	fbsLane      = 5
	fbsKind      = 6
	fbsPostScope = 11 // the scope oneof is 10 (page) to 13 (media)
	fbsItemsSeen = 20
	fbsKindPost  = 2 // the kinds are 1 (page) to 4 (media)
)

// fbs's and igs's tasks cannot be mistaken for each other: an fbs task on
// igs's topic decodes with an unknown kind and no scope, so the worker
// dead-letters it instead of running it, and igs's kinds and scope fields
// are none of fbs's, so fbs reads an igs task the same way (ADR 0005 §2).
func TestTasksAreDisjoint(t *testing.T) {
	var post []byte
	post = protowire.AppendTag(post, 1, protowire.VarintType) // post_id
	post = protowire.AppendVarint(post, 7)
	var fb []byte
	fb = protowire.AppendTag(fb, fbsTaskID, protowire.BytesType)
	fb = protowire.AppendString(fb, "t")
	fb = protowire.AppendTag(fb, fbsJobID, protowire.BytesType)
	fb = protowire.AppendString(fb, "j")
	fb = protowire.AppendTag(fb, fbsLane, protowire.VarintType)
	fb = protowire.AppendVarint(fb, uint64(igstaskv1.Lane_LANE_P1))
	fb = protowire.AppendTag(fb, fbsKind, protowire.VarintType)
	fb = protowire.AppendVarint(fb, fbsKindPost)
	fb = protowire.AppendTag(fb, fbsPostScope, protowire.BytesType)
	fb = protowire.AppendBytes(fb, post)
	fb = protowire.AppendTag(fb, fbsItemsSeen, protowire.VarintType)
	fb = protowire.AppendVarint(fb, 3)
	var ig igstaskv1.Task
	if err := proto.Unmarshal(fb, &ig); err != nil {
		t.Fatal(err)
	}
	if _, ok := TaskTopic(ig.Kind, ig.Lane); ok || ig.Scope != nil || ig.TaskId != "t" || ig.ItemsSeen != 3 {
		t.Fatalf("an fbs task read as igs's: kind %v scope %v", ig.Kind, ig.Scope)
	}
	for _, k := range []igstaskv1.Kind{igstaskv1.Kind_KIND_PROFILE, igstaskv1.Kind_KIND_POST, igstaskv1.Kind_KIND_MEDIA} {
		if k >= 1 && k <= 4 {
			t.Errorf("igs kind %v (%d) is also an fbs kind", k, k)
		}
	}
	scope := (&igstaskv1.Task{}).ProtoReflect().Descriptor().Oneofs().ByName("scope")
	for i := range scope.Fields().Len() {
		if n := scope.Fields().Get(i).Number(); n >= 10 && n <= 13 {
			t.Errorf("igs scope field %s has fbs's scope number %d", scope.Fields().Get(i).Name(), n)
		}
	}
}

// Against a real Schema Registry (IGS_TEST_SCHEMA_REGISTRY): igs's contract
// registers, under BACKWARD_TRANSITIVE, and applying it again changes
// nothing.
func TestSchemasApply(t *testing.T) {
	u := os.Getenv("IGS_TEST_SCHEMA_REGISTRY")
	if u == "" {
		t.Skip("IGS_TEST_SCHEMA_REGISTRY not set")
	}
	c := &schemareg.Client{URL: u}
	ctx := context.Background()
	prefix := fmt.Sprintf("ig%d.", rand.Int64N(1<<40))
	t.Cleanup(func() {
		subjects, _ := c.Subjects(context.Background())
		sort.Slice(subjects, func(i, j int) bool {
			return strings.HasSuffix(subjects[i], "-value") && !strings.HasSuffix(subjects[j], "-value")
		})
		for _, s := range subjects {
			if strings.HasPrefix(s, prefix) {
				_ = c.DeleteSubject(context.Background(), s)
			}
		}
	})
	res, err := schemareg.Apply(ctx, c, protosrc.FS, prefix, ValueSchemas, false)
	if err != nil {
		t.Fatal(err)
	}
	values := 0
	for _, r := range res {
		if strings.HasSuffix(r.Subject, "-value") {
			values++
		}
		if r.Version != 1 {
			t.Errorf("%s: version %d", r.Subject, r.Version)
		}
	}
	if values != len(ValueSchemas) {
		t.Fatalf("%d value subjects, want %d: %+v", values, len(ValueSchemas), res)
	}
	if lvl, err := c.Compatibility(ctx, prefix+Comment+"-value"); err != nil || lvl != schemareg.BackwardTransitive {
		t.Fatalf("compatibility %q %v", lvl, err)
	}
	again, err := schemareg.Apply(ctx, c, protosrc.FS, prefix, ValueSchemas, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range again {
		if !r.Unchanged {
			t.Errorf("%s changed on a second apply", r.Subject)
		}
	}
}
