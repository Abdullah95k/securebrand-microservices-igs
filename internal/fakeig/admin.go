package fakeig

import (
	"encoding/json"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"
)

// AdminPrefix is where a server with Options.Admin answers the soak's
// driver (cmd/igs-soak): the profiles, world changes and ground truth.
// Instagram has nothing like it; fakeig serves it only when asked
// (fakeig -admin).
const AdminPrefix = "/__fakeig/"

// Mutate makes one random change of the kinds igs's soak exercises, as
// accounts posting between two scheduled runs do: a new post on a profile
// (3 in 10), 1 to 4 comments on one of a profile's ten newest posts (4 in
// 10), or more likes on one of its posts (3 in 10). It returns what it
// changed ("" when the change did not apply, a walled post's comments).
func (w *World) Mutate(r *rand.Rand) (string, error) {
	w.mu.RLock()
	if len(w.profiles) == 0 {
		w.mu.RUnlock()
		return "", nil
	}
	p := w.profiles[r.IntN(len(w.profiles))]
	name, n := p.Username, r.IntN(10)
	var post *Post
	switch {
	case len(p.Posts) == 0:
	case n >= 3 && n < 7:
		post = p.Posts[r.IntN(min(len(p.Posts), 10))]
	default:
		post = p.Posts[r.IntN(len(p.Posts))]
	}
	var pk, likes int64
	gated := false
	if post != nil {
		pk, likes, gated = post.PK, post.Likes, post.Gated
	}
	w.mu.RUnlock()
	switch {
	case n < 3:
		_, err := w.AddPosts(name, 1, time.Minute)
		return "new posts", err
	case post == nil || (n < 7 && gated):
		return "", nil
	case n < 7:
		return "comments on recent posts", w.AddComments(pk, 1+r.IntN(4))
	default:
		return "like counts", w.SetLikes(pk, likes+int64(1+r.IntN(50)))
	}
}

// Usernames lists the world's profiles.
func (w *World) Usernames() []string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	out := make([]string, len(w.profiles))
	for i, p := range w.profiles {
		out[i] = p.Username
	}
	return out
}

// admin serves AdminPrefix:
//   - GET profiles: the usernames, as a JSON list;
//   - POST mutate?n=N: N random changes (World.Mutate), answering how many
//     of each kind;
//   - GET truth?since=RFC3339: every profile's ProfileTruth back to since;
//   - GET instance: {"started": RFC3339} when this server started. A
//     restart generates the world again from its seed, so a driver that
//     sees it change knows the truth is no longer the world igs read.
func (s *Server) admin(w http.ResponseWriter, r *http.Request) {
	reply := func(v any) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(v)
	}
	switch name := r.URL.Path[len(AdminPrefix):]; {
	case name == "profiles" && r.Method == http.MethodGet:
		reply(s.World.Usernames())
	case name == "mutate" && r.Method == http.MethodPost:
		n, _ := strconv.Atoi(r.URL.Query().Get("n"))
		changes := map[string]int{}
		s.mutateMu.Lock()
		if s.rng == nil {
			s.rng = rand.New(rand.NewPCG(s.World.spec.Seed, 0x50a4))
		}
		for range max(n, 1) {
			what, err := s.World.Mutate(s.rng)
			if err != nil {
				s.mutateMu.Unlock()
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if what != "" {
				changes[what]++
			}
		}
		s.mutateMu.Unlock()
		reply(changes)
	case name == "instance" && r.Method == http.MethodGet:
		reply(map[string]string{"started": s.started})
	case name == "truth" && r.Method == http.MethodGet:
		since, err := time.Parse(time.RFC3339, r.URL.Query().Get("since"))
		if err != nil {
			http.Error(w, "since: an RFC 3339 time", http.StatusBadRequest)
			return
		}
		var out []ProfileTruth
		for _, u := range s.World.Usernames() {
			if t, ok := s.World.Truth(u, since); ok {
				out = append(out, t)
			}
		}
		reply(out)
	default:
		http.NotFound(w, r)
	}
}
