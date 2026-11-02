package transport_test

import (
	"testing"

	"github.com/bogdanfinn/tls-client/profiles"

	"github.com/abdullah95k/securebrand-microservices-igs/configs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/session"
)

// Every fingerprint the configuration rotates through has a TLS profile in
// the pinned tls-client and an entry in fingerprints.yaml, so no identity
// pairs a user agent with another browser's handshake.
func TestConfiguredFingerprintsExist(t *testing.T) {
	b, err := configs.FS.ReadFile("fingerprints.yaml")
	if err != nil {
		t.Fatal(err)
	}
	fps, err := session.LoadFingerprints(b)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	names := append([]string{cfg.FB.FingerprintProfile}, cfg.FB.FingerprintProfiles...)
	if len(cfg.FB.FingerprintProfiles) < 2 {
		t.Fatalf("the default rotates through several profiles: %v", cfg.FB.FingerprintProfiles)
	}
	for _, n := range names {
		if _, ok := fps[n]; !ok {
			t.Errorf("%s: not in fingerprints.yaml", n)
		}
		if _, ok := profiles.MappedTLSClients[n]; !ok {
			t.Errorf("%s: no tls-client profile", n)
		}
	}
	for n := range fps {
		if _, ok := profiles.MappedTLSClients[n]; !ok {
			t.Errorf("fingerprints.yaml %s: no tls-client profile", n)
		}
	}
}
