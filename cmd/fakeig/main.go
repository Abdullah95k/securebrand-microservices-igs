// Command fakeig serves a synthetic logged-out Instagram (internal/fakeig)
// for rehearsals: igs-spike phase0's dry run (make igs-phase0-dryrun) and
// local runs of igs's services. Everything it serves is generated.
package main

import (
	"flag"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/fakeig"
	igclient "github.com/abdullah95k/securebrand-microservices-igs/internal/ig/client"
)

func main() {
	addr := flag.String("addr", ":8091", "listen address")
	seed := flag.Uint64("seed", 1, "world seed")
	profiles := flag.Int("profiles", 5, "number of profiles (profile_01, profile_02, ...)")
	posts := flag.Int("posts", 40, "posts per profile")
	comments := flag.Int("max-comments", 25, "max comments per post")
	docIDs := flag.String("docids", "seed", "accepted doc_ids: seed (the embedded registry seed) or rotated (other ids, discoverable from the page's /rsrc.php bundle)")
	rateLimitEvery := flag.Int("rate-limit-every", 0, "answer every Nth GraphQL request with HTTP 429 (0 = off)")
	loginWallAfter := flag.Int("login-wall-after", 0, "GraphQL requests per identity before a login wall (0 = off)")
	flag.Parse()

	spec := fakeig.DefaultSpec()
	spec.Seed, spec.Profiles, spec.PostsPerProfile, spec.MaxComments = *seed, *profiles, *posts, *comments
	spec.Pinned, spec.GatedEvery, spec.HiddenLikesEvery, spec.RepliesEvery = 2, 9, 5, 4
	reg, err := igclient.Seed()
	if err != nil {
		slog.Error("fakeig: seed", "err", err)
		os.Exit(1)
	}
	docs := map[string][]string{}
	for _, q := range reg.Queries {
		id := q.DocID
		if *docIDs == "rotated" {
			id = "9" + id[1:]
		}
		docs[q.FriendlyName] = append(docs[q.FriendlyName], id)
	}
	srv := fakeig.NewServer(fakeig.NewWorld(spec), fakeig.Options{Docs: docs,
		Faults: fakeig.Faults{RateLimitEvery: *rateLimitEvery, LoginWallAfter: *loginWallAfter}})
	slog.Info("fakeig listening", "addr", *addr, "profiles", *profiles, "docids", *docIDs)
	hs := &http.Server{Addr: *addr, Handler: srv, ReadHeaderTimeout: 10 * time.Second}
	if err := hs.ListenAndServe(); err != nil {
		slog.Error("fakeig", "err", err)
		os.Exit(1)
	}
}
