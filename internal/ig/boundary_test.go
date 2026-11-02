package ig_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const module = "github.com/abdullah95k/securebrand-microservices-igs"

// forbidden are Facebook's packages: Instagram code builds on the neutral
// layer (internal/scrape) only, so the copied Facebook code can be removed
// later without touching it (ADR 0004 §2).
var forbidden = []string{
	module + "/internal/fb",
	module + "/internal/fakefb",
	module + "/gen/go/securebrand/fb/",
}

func TestNoFacebookImports(t *testing.T) {
	fset := token.NewFileSet()
	files := 0
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		files++
		for _, imp := range f.Imports {
			p, _ := strconv.Unquote(imp.Path.Value)
			for _, bad := range forbidden {
				if p == strings.TrimSuffix(bad, "/") || strings.HasPrefix(p, strings.TrimSuffix(bad, "/")+"/") {
					t.Errorf("%s imports %s", path, p)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if files == 0 {
		t.Fatal("no Go files found")
	}
}

// Nor through another package: igs's code and binaries must build with the
// copied Facebook code deleted (ADR 0004 §2), so no Facebook package may be
// among their dependencies.
func TestNoFacebookDependencies(t *testing.T) {
	gobin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go not on PATH")
	}
	out, err := exec.Command(gobin, "list", "-deps", "./...", "../../cmd/igs-crawler", "../../cmd/igs-persister", "../../cmd/igs-api", "../../cmd/igs-scheduler").CombinedOutput()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, out)
	}
	for _, dep := range strings.Fields(string(out)) {
		for _, bad := range append(forbidden, module+"/gen/go/securebrand/fbs/", module+"/internal/topics", module+"/internal/gdpr",
			module+"/internal/crawl/stage", module+"/internal/crawl/emit", module+"/internal/persist", module+"/internal/api",
			module+"/internal/dispatch", module+"/internal/scheduler", module+"/internal/media", module+"/internal/app") {
			if dep == strings.TrimSuffix(bad, "/") || strings.HasPrefix(dep, strings.TrimSuffix(bad, "/")+"/") {
				t.Errorf("igs depends on %s", dep)
			}
		}
	}
}
