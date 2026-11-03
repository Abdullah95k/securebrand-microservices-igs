package config

import (
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

// The chart renders its values' config: into igs's config file. Each values
// file the repository ships must give a config igs accepts, with the
// secrets the chart takes from Secrets set as environment variables.
func TestChartValuesConfigLoads(t *testing.T) {
	dir := "../../deploy/helm/igs"
	for _, f := range []string{"values.yaml", "examples/fakeig.yaml", "examples/production.yaml"} {
		t.Run(f, func(t *testing.T) {
			b, err := os.ReadFile(filepath.Join(dir, f))
			if err != nil {
				t.Fatal(err)
			}
			var v struct {
				Config map[string]any `yaml:"config"`
			}
			if err := yaml.Unmarshal(b, &v); err != nil {
				t.Fatal(err)
			}
			out, err := yaml.Marshal(v.Config)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, out, 0o600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("IGS_IG__PRIVACY__AUTHOR_HMAC_KEY", "a-test-key-that-is-not-the-default")
			c, err := Load(path)
			if err != nil {
				t.Fatalf("%s: %v", f, err)
			}
			if f == "examples/production.yaml" && (c.IG.ExitRPS != 0.25 || c.Service.Env != "prod" || c.IG.Proxy.Routing["default"][0] != "isp") {
				t.Fatalf("production: exit_rps %v env %s routing %v", c.IG.ExitRPS, c.Service.Env, c.IG.Proxy.Routing)
			}
		})
	}
}
