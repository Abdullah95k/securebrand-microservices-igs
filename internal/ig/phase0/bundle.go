package phase0

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// ErrSecretInBundle stops the bundle when a configured credential shows up
// in a file.
var ErrSecretInBundle = errors.New("a proxy credential is in a file meant for the bundle")

// bundleFile reports whether a file of the output directory belongs in the
// bundle: the kit's own anonymous results and scrubbed fixtures, nothing
// else (never the raw capture, the config or the probe's personal output).
func bundleFile(rel string) bool {
	switch rel {
	case FilePreflight, FileDocIDs, FileRegistry, FileFixtures, FileReportTxt, FileReport, FileMeta:
		return true
	}
	if strings.HasPrefix(rel, DirFixtures+"/") {
		return true
	}
	return (strings.HasPrefix(rel, "smoke-") || strings.HasPrefix(rel, "run-")) && strings.HasSuffix(rel, ".json")
}

// bundle packs the results into igs-phase0-bundle.tar.gz, for the owner to
// send back.
func (k *Kit) bundle() error {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	n := 0
	err := filepath.WalkDir(k.Cfg.Out, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(k.Cfg.Out, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if !bundleFile(rel) {
			return nil
		}
		b, err := os.ReadFile(path) //nolint:gosec // the kit's own output directory
		if err != nil {
			return err
		}
		for _, s := range k.Cfg.Secrets {
			if len(s) >= 4 && bytes.Contains(b, []byte(s)) {
				return fmt.Errorf("%w: %s", ErrSecretInBundle, rel)
			}
		}
		if err := tw.WriteHeader(&tar.Header{Name: "igs-phase0/" + rel, Mode: 0o600, Size: int64(len(b))}); err != nil {
			return err
		}
		if _, err := tw.Write(b); err != nil {
			return err
		}
		n++
		return nil
	})
	if err != nil {
		return &StepError{Step: StepBundle, Err: err}
	}
	if err := tw.Close(); err != nil {
		return err
	}
	if err := gz.Close(); err != nil {
		return err
	}
	if err := os.WriteFile(k.path(FileBundle), buf.Bytes(), 0o600); err != nil {
		return err
	}
	k.logf("bundle: %s (%d files, %d KB)", k.path(FileBundle), n, buf.Len()/1024)
	return nil
}
