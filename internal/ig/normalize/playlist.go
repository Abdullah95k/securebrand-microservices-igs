package normalize

import (
	contentv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/content/v1"

	commonv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/common/v1"
	igv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/ig/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/topics"
)

// HighlightURL is a highlight's page on Instagram.
func HighlightURL(id int64) string {
	return "https://www.instagram.com/stories/highlights/" + pk(id) + "/"
}

// AudioURL is a sound's page on Instagram.
func AudioURL(id string) string { return "https://www.instagram.com/reels/audio/" + id + "/" }

// playlist maps ig.highlight.v1 to content.playlist.v1 (REQ-019 slice 3):
// one Playlist of type highlight per highlight, keyed by its ID. Its items
// are posts of type story, published only once a real check shows
// logged-out Instagram serves them (slice 3.2), so until then the playlist
// carries their count, not references to posts that don't exist yet. The
// cover has no media record yet, so it is left out.
func playlist(h *igv1.Highlight) []Out {
	if h.HighlightId == 0 || h.UserPk == 0 {
		return nil
	}
	env := envelope(h.Obs, KindPlaylist, KindPlaylist, pk(h.HighlightId))
	n := int64(h.ItemCount)
	if n == 0 {
		n = int64(len(h.Items))
	}
	c := &contentv1.Playlist{Envelope: env, AccountId: ID(KindAccount, pk(h.UserPk)), Type: contentv1.PlaylistType_PLAYLIST_TYPE_HIGHLIGHT,
		Title: Text(h.Title), ItemCount: exact(&n), Status: &contentv1.Status{Availability: contentv1.Availability_AVAILABILITY_VISIBLE}, Url: HighlightURL(h.HighlightId)}
	seal(env, c)
	return []Out{{Topic: topics.ContentPlaylist, Key: env.Id, Msg: c}}
}

// audio maps a post's sound to content.audio.v1, keyed by its ID, with the
// post's observation. Instagram names a reel's sound on its page; original
// audio is the owner's own sound.
func audio(o *commonv1.Observation, m *igv1.Music) []Out {
	if m == nil || m.AudioId == "" {
		return nil
	}
	env := envelope(o, KindAudio, KindAudio, m.AudioId)
	original := m.OriginalAudio
	c := &contentv1.Audio{Envelope: env, Title: m.Title, Artist: m.Artist, Original: &original, Url: AudioURL(m.AudioId),
		Status: &contentv1.Status{Availability: contentv1.Availability_AVAILABILITY_VISIBLE}}
	seal(env, c)
	return []Out{{Topic: topics.ContentAudio, Key: env.Id, Msg: c}}
}
