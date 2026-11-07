package topics

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/abdullah95k/securebrand-microservices-igs/configs"
)

type chartValues struct {
	Components map[string]struct {
		Autoscale *struct {
			Min int `yaml:"min"`
			Max int `yaml:"max"`
			Lag []struct {
				Topic string `yaml:"topic"`
			} `yaml:"lag"`
		} `yaml:"autoscale"`
	} `yaml:"components"`
}

func readValues(t *testing.T, file string) chartValues {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("../../../deploy/helm/igs", file))
	if err != nil {
		t.Fatal(err)
	}
	var v chartValues
	if err := yaml.Unmarshal(b, &v); err != nil {
		t.Fatalf("%s: %v", file, err)
	}
	return v
}

// No component scales to more pods than the largest topic it consumes has
// partitions (REQ-008 fix 20): a consumer group's pods past that only
// wait. The chart's defaults and every shipped values file are checked
// against configs/ig/topics.yaml, which holds the production sizes. The
// platform's content.* topics are left out: their sizes are not igs's.
func TestKEDAMaximaFitTopics(t *testing.T) {
	b, err := configs.FS.ReadFile("ig/topics.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Topics []struct {
			Name       string `yaml:"name"`
			Partitions int    `yaml:"partitions"`
		} `yaml:"topics"`
	}
	if err := yaml.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	partitions := map[string]int{}
	for _, tp := range doc.Topics {
		partitions[tp.Name] = tp.Partitions
	}
	defaults := readValues(t, "values.yaml")
	for _, file := range []string{"values.yaml", "values-production.yaml", "examples/production.yaml"} {
		v := readValues(t, file)
		for name, c := range defaults.Components {
			if c.Autoscale == nil {
				continue
			}
			largest := 0
			for _, l := range c.Autoscale.Lag {
				if strings.HasPrefix(l.Topic, "content.") {
					continue
				}
				n, ok := partitions[l.Topic]
				if !ok {
					t.Errorf("%s: autoscales on %s, which is not in configs/ig/topics.yaml", name, l.Topic)
				}
				largest = max(largest, n)
			}
			if largest == 0 {
				continue // only the platform's topics
			}
			lo, hi := c.Autoscale.Min, c.Autoscale.Max
			if o, ok := v.Components[name]; ok && o.Autoscale != nil {
				if o.Autoscale.Max > 0 {
					hi = o.Autoscale.Max
				}
				if o.Autoscale.Min > 0 {
					lo = o.Autoscale.Min
				}
			}
			if hi > largest || lo > hi {
				t.Errorf("%s: %s scales %d to %d pods; its largest topic has %d partitions", file, name, lo, hi, largest)
			}
		}
	}
}
