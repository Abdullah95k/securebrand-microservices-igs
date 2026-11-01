package probe

import (
	"archive/tar"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Write puts the report in dir: report.json, and profiles.csv, posts.csv,
// comments.csv and reels.csv for a spreadsheet. Every file is 0600: the report holds
// personal data. The profile pictures are already in dir/images.
func Write(dir string, rep Report) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	b, _ := json.MarshalIndent(rep, "", "  ")
	if err := os.WriteFile(filepath.Join(dir, "report.json"), append(b, '\n'), 0o600); err != nil {
		return err
	}
	tables := map[string][][]string{"profiles.csv": {profilesHeader}, "posts.csv": {postsHeader}, "comments.csv": {commentsHeader},
		"reels.csv": {reelsHeader}}
	for _, p := range rep.Profiles {
		tables["profiles.csv"] = append(tables["profiles.csv"], profileRow(p))
		tables["posts.csv"] = append(tables["posts.csv"], postRows(p)...)
		tables["comments.csv"] = append(tables["comments.csv"], commentRows(p)...)
		tables["reels.csv"] = append(tables["reels.csv"], reelRows(p)...)
	}
	for name, rows := range tables {
		if err := writeCSV(filepath.Join(dir, name), rows); err != nil {
			return err
		}
	}
	return nil
}

// Saver keeps the three CSVs current while a run goes: each profile's rows
// are appended as soon as it is read, so the files can be opened (or
// counted) mid-run, and a run that dies keeps what it read. Write rewrites
// them at the end, with report.json.
type Saver struct {
	files []*os.File
	w     []*csv.Writer
}

// NewSaver creates dir and starts the CSVs, with their headers.
func NewSaver(dir string) (*Saver, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	s := &Saver{}
	for _, t := range []struct {
		name   string
		header []string
	}{{"profiles.csv", profilesHeader}, {"posts.csv", postsHeader}, {"comments.csv", commentsHeader}, {"reels.csv", reelsHeader}} {
		f, err := os.OpenFile(filepath.Join(dir, t.name), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600) //nolint:gosec // the operator's output dir
		if err != nil {
			_ = s.Close()
			return nil, err
		}
		s.files, s.w = append(s.files, f), append(s.w, csv.NewWriter(f))
		if err := s.write(len(s.w)-1, [][]string{t.header}); err != nil {
			_ = s.Close()
			return nil, err
		}
	}
	return s, nil
}

// Add appends one profile's rows.
func (s *Saver) Add(p Profile) error {
	for i, rows := range [][][]string{{profileRow(p)}, postRows(p), commentRows(p), reelRows(p)} {
		if err := s.write(i, rows); err != nil {
			return err
		}
	}
	return nil
}

func (s *Saver) write(i int, rows [][]string) error {
	return s.w[i].WriteAll(rows) // WriteAll flushes
}

// Close closes the files.
func (s *Saver) Close() error {
	var err error
	for _, f := range s.files {
		if cerr := f.Close(); err == nil {
			err = cerr
		}
	}
	return err
}

var (
	profilesHeader = []string{"line", "username", "url", "full_name", "followers", "following", "verified", "private",
		"biography", "bio_links", "linked_facebook_page", "highlights", "profile_pic_file", "profile_pic_url",
		"page_load", "posts_read", "grid_stop", "oldest_post_read", "comments_read"}
	postsHeader = []string{"username", "shortcode", "url", "posted_at", "media_type", "product_type", "caption", "alt_text", "thumbnail_url",
		"pinned", "details", "plays", "likes", "comment_count", "taken_at", "caption_edited", "likes_hidden", "tagged", "coauthors", "location",
		"music", "topics", "duration_s", "width", "height", "has_audio", "ai_label", "carousel_count", "video_url", "image_url"}
	commentsHeader = []string{"username", "post_shortcode", "commenter", "commenter_verified", "created_at", "likes", "text"}
	reelsHeader    = []string{"username", "shortcode", "url", "posted_at", "plays", "likes", "comment_count", "counts_hidden", "pinned", "thumbnail_url"}
)

func reelRows(p Profile) [][]string {
	var rows [][]string
	for _, x := range p.Reels {
		posted := ""
		if !x.PostedAt.IsZero() {
			posted = x.PostedAt.Format(time.RFC3339)
		}
		rows = append(rows, []string{p.Username, x.Shortcode, x.URL, posted, i64p(x.Plays), i64p(x.Likes), i64p(x.Comments),
			yes(x.CountsHidden), yes(x.Pinned), x.ThumbnailURL})
	}
	return rows
}

func profileRow(p Profile) []string {
	row := []string{strconv.Itoa(p.Index), p.Username, p.URL}
	if d := p.Data; d != nil {
		var links, hl []string
		for _, l := range d.BioLinks {
			links = append(links, l.URL)
		}
		for _, h := range d.Highlights {
			hl = append(hl, h.Title)
		}
		row = append(row, d.FullName, i64(d.Followers), i64(d.Following), strconv.FormatBool(d.Verified),
			strconv.FormatBool(d.Private), d.Biography, strings.Join(links, " "), d.LinkedFacebookPage,
			strings.Join(hl, " | "), d.ProfilePicFile, d.ProfilePicURL)
	} else {
		row = append(row, make([]string, 11)...)
	}
	stop, oldest := "", ""
	if p.Grid != nil {
		stop = p.Grid.Stop
		if !p.Grid.Oldest.IsZero() {
			oldest = p.Grid.Oldest.Format(time.RFC3339)
		}
	}
	return append(row, p.Bootstrap.Class, strconv.Itoa(len(p.Posts)), stop, oldest, strconv.Itoa(len(p.Comments)))
}

func postRows(p Profile) [][]string {
	var rows [][]string
	for _, x := range p.Posts {
		posted := ""
		if !x.PostedAt.IsZero() {
			posted = x.PostedAt.Format(time.RFC3339)
		}
		row := []string{p.Username, x.Shortcode, x.URL, posted, x.MediaType, x.ProductType, x.Caption, x.AltText, x.ThumbnailURL, yes(x.Pinned)}
		class := ""
		if x.Details != nil {
			class = x.Details.Class
		}
		row = append(row, class, i64p(x.Plays), i64p(x.Likes), i64p(x.Comments))
		if d := x.Details; d != nil {
			taken := ""
			if !d.TakenAt.IsZero() {
				taken = d.TakenAt.Format(time.RFC3339)
			}
			dur := ""
			if d.DurationS > 0 {
				dur = strconv.FormatFloat(d.DurationS, 'f', -1, 64)
			}
			row = append(row, taken, yes(d.CaptionEdited), yes(d.LikesHidden),
				strings.Join(d.Tagged, " | "), strings.Join(d.Coauthors, " | "), d.Location, d.Music, strings.Join(d.Topics, " | "),
				dur, nz(d.Width), nz(d.Height), yes(d.HasAudio), d.AILabel, nz(d.CarouselCount), d.VideoURL, d.ImageURL)
		} else {
			row = append(row, make([]string, 16)...)
		}
		rows = append(rows, row)
	}
	return rows
}

func commentRows(p Profile) [][]string {
	var rows [][]string
	for _, c := range p.Comments {
		rows = append(rows, []string{p.Username, c.PostShortcode, c.Username, strconv.FormatBool(c.Verified),
			c.CreatedAt.Format(time.RFC3339), i64(c.Likes), c.Text})
	}
	return rows
}

func writeCSV(path string, rows [][]string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600) //nolint:gosec // the operator's output dir
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	if err := w.WriteAll(rows); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// Bundle packs dir into dir.tar.gz (0600) for downloading, and returns its
// path.
func Bundle(dir string) (string, error) {
	out := strings.TrimRight(dir, "/") + ".tar.gz"
	f, err := os.OpenFile(out, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600) //nolint:gosec // the operator's output dir
	if err != nil {
		return "", err
	}
	gz := gzip.NewWriter(f)
	tw := tar.NewWriter(gz)
	root := filepath.Base(dir)
	err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(dir, path)
		if err := tw.WriteHeader(&tar.Header{Name: filepath.ToSlash(filepath.Join(root, rel)), Mode: 0o600,
			Size: info.Size(), ModTime: info.ModTime()}); err != nil {
			return err
		}
		src, err := os.Open(path) //nolint:gosec // walking our own output dir
		if err != nil {
			return err
		}
		defer func() { _ = src.Close() }()
		_, err = io.Copy(tw, src)
		return err
	})
	for _, c := range []io.Closer{tw, gz, f} {
		if cerr := c.Close(); err == nil {
			err = cerr
		}
	}
	if err != nil {
		return "", fmt.Errorf("probe: bundle: %w", err)
	}
	return out, nil
}

func i64(v int64) string { return strconv.FormatInt(v, 10) }

func i64p(v *int64) string {
	if v == nil {
		return ""
	}
	return i64(*v)
}

func yes(b bool) string {
	if b {
		return "yes"
	}
	return ""
}

func nz(n int) string {
	if n == 0 {
		return ""
	}
	return strconv.Itoa(n)
}
