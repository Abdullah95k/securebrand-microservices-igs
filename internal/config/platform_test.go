package config

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/url"
	"strings"
	"testing"
	"time"
)

// The platform's credentials Secret (platform-credentials) reaches igs as
// its own variable names; with IGS_PLATFORM_CREDENTIALS set, Load fills
// Postgres, Kafka, Valkey, S3 and the schema registry from them (REQ-008
// fix 11a). Values are fakes.
func TestPlatformCredentials(t *testing.T) {
	env := map[string]string{
		PlatformEnv:         "true",
		"POSTGRES_HOST":     "pg.platform.svc",
		"POSTGRES_PORT":     "6432",
		"POSTGRES_DATABASE": "igs",
		"POSTGRES_USER":     "igs",
		// Characters a URL must escape.
		"POSTGRES_PASSWORD":    "p@ss:w/rd?#%",
		"POSTGRES_SSLMODE":     "require",
		"KAFKA_BROKERS":        "k1:9092, k2:9092",
		"KAFKA_USER":           "igs",
		"KAFKA_PASSWORD":       "kpass",
		"KAFKA_SASL_MECHANISM": "SCRAM-SHA-512",
		"KAFKA_TLS":            "true",
		"VALKEY_HOST":          "valkey.platform.svc",
		"VALKEY_PASSWORD":      "vpass",
		"S3_ENDPOINT":          "https://s3.platform.example",
		"S3_REGION":            "me-central-1",
		"S3_ACCESS_KEY":        "AKIAFAKE",
		"S3_SECRET_KEY":        "fake-secret",
		"S3_BUCKET_RAW":        "platform-raw",
		"S3_BUCKET_MEDIA":      "platform-media",
		"S3_BUCKET_BACKUPS":    "platform-backups",
		"SCHEMA_REGISTRY_URL":  "http://registry.platform.svc:8081",
		"CONTRACT_VERSION":     "1.0.0",
	}
	for k, v := range env {
		t.Setenv(k, v)
	}
	c, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(c.Postgres.DSN)
	if err != nil {
		t.Fatalf("DSN does not parse: %v", err)
	}
	if pw, _ := u.User.Password(); pw != env["POSTGRES_PASSWORD"] || u.User.Username() != "igs" || u.Host != "pg.platform.svc:6432" ||
		u.Path != "/igs" || u.Query().Get("sslmode") != "require" {
		t.Errorf("postgres: user %q host %q path %q sslmode %q (password round trip %v)", u.User.Username(), u.Host, u.Path,
			u.Query().Get("sslmode"), pw == env["POSTGRES_PASSWORD"])
	}
	k := c.Kafka
	if strings.Join(k.Brokers, ",") != "k1:9092,k2:9092" || k.SASL.Username != "igs" || k.SASL.Password != "kpass" ||
		k.SASL.Mechanism != "scram-sha-512" || !k.TLS.Enabled || k.SchemaRegistry.URL != env["SCHEMA_REGISTRY_URL"] {
		t.Errorf("kafka: brokers %v, sasl %q/%q, tls %v, registry %q", k.Brokers, k.SASL.Username, k.SASL.Mechanism, k.TLS.Enabled,
			k.SchemaRegistry.URL)
	}
	if c.Redis.Addr != "valkey.platform.svc:6379" || c.Redis.Password != "vpass" {
		t.Errorf("valkey: %q", c.Redis.Addr)
	}
	s := c.S3
	if s.Endpoint != "s3.platform.example" || !s.UseSSL || s.Region != "me-central-1" || s.AccessKey != "AKIAFAKE" ||
		s.SecretKey != "fake-secret" || s.RawBucket != "platform-raw" || s.MediaBucket != "platform-media" ||
		s.BackupsBucket != "platform-backups" {
		t.Errorf("s3: %+v", s)
	}
	if c.Platform.ContractVersion != "1.0.0" {
		t.Errorf("contract version %q", c.Platform.ContractVersion)
	}
}

// Valkey may come as a URL or an address; without the switch, the
// platform's variables are ignored; with it, a missing one is named
// (never its value) and Load fails.
func TestPlatformCredentialsVariants(t *testing.T) {
	base := map[string]string{"POSTGRES_HOST": "pg", "POSTGRES_DATABASE": "igs", "POSTGRES_USER": "igs", "POSTGRES_PASSWORD": "x",
		"KAFKA_BROKERS": "k:9092", "S3_ENDPOINT": "s3:9000", "S3_ACCESS_KEY": "a", "S3_SECRET_KEY": "s",
		"S3_BUCKET_RAW": "r", "S3_BUCKET_MEDIA": "m"}
	apply := func(extra map[string]string) (*Config, error) {
		env := map[string]string{}
		for k, v := range base {
			env[k] = v
		}
		for k, v := range extra {
			env[k] = v
		}
		c, err := Load("")
		if err != nil {
			t.Fatal(err)
		}
		return c, c.applyPlatform(func(k string) string { return env[k] })
	}
	c, err := apply(map[string]string{"VALKEY_URL": "rediss://:secret@valkey.svc:6380/2"})
	if err != nil || c.Redis.Addr != "valkey.svc:6380" || c.Redis.Password != "secret" || !c.Redis.TLS.Enabled || c.Redis.DB != 2 {
		t.Errorf("VALKEY_URL: %q db %d tls %v, %v", c.Redis.Addr, c.Redis.DB, c.Redis.TLS.Enabled, err)
	}
	if c.S3.Endpoint != "s3:9000" {
		t.Errorf("an endpoint without a scheme is kept as is: %q", c.S3.Endpoint)
	}
	if c, err = apply(map[string]string{"VALKEY_ADDR": "v:6379"}); err != nil || c.Redis.Addr != "v:6379" {
		t.Errorf("VALKEY_ADDR: %q, %v", c.Redis.Addr, err)
	}
	if _, err = apply(map[string]string{"POSTGRES_PASSWORD": "", "VALKEY_ADDR": "v:6379"}); err == nil ||
		!strings.Contains(err.Error(), "POSTGRES_PASSWORD") {
		t.Errorf("a missing password: %v", err)
	}
	// A component given no object-store key (REQ-008 fix 22b) still loads,
	// with no key at all, not the config file's development one.
	c, err = apply(map[string]string{"VALKEY_ADDR": "v:6379", "S3_ACCESS_KEY": "", "S3_SECRET_KEY": ""})
	if err != nil || c.S3.AccessKey != "" || c.S3.SecretKey != "" || c.S3.RawBucket != "r" {
		t.Errorf("no S3 key: key %q, bucket %q, %v", c.S3.AccessKey, c.S3.RawBucket, err)
	}
	if _, err = apply(nil); err == nil || !strings.Contains(err.Error(), "VALKEY_HOST") {
		t.Errorf("no Valkey address: %v", err)
	}

	// Without the switch, Load leaves the platform's names alone.
	t.Setenv("POSTGRES_HOST", "pg.platform.svc")
	c, err = Load("")
	if err != nil || strings.Contains(c.Postgres.DSN, "pg.platform.svc") {
		t.Errorf("platform variables used without %s: %q, %v", PlatformEnv, c.Postgres.DSN, err)
	}
}

// Prefixes that would let igs delete or expire what is not its own are
// refused.
func TestS3PrefixesValidated(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*S3)
		want string
	}{
		{"no media prefix", func(s *S3) { s.MediaPrefix = "" }, "s3.media_prefix"},
		{"media prefix without slash", func(s *S3) { s.MediaPrefix = "igs" }, "s3.media_prefix"},
		{"keep inside raw", func(s *S3) { s.RawPrefix, s.RawKeepPrefix = "igs/", "igs/keep/" }, "s3.raw_prefix"},
		{"same prefix", func(s *S3) { s.RawPrefix, s.RawKeepPrefix = "igs/raw/", "igs/raw/" }, "s3.raw_prefix"},
		{"sample inside raw", func(s *S3) { s.RawPrefix, s.RawSamplePrefix = "igs/30d/", "igs/30d/sample/" }, "s3.raw_sample_prefix"},
		{"failures inside the sample", func(s *S3) { s.RawSamplePrefix, s.RawKeepPrefix = "igs/keep/", "igs/keep/failures/" }, "s3.raw_sample_prefix"},
		{"no sample prefix", func(s *S3) { s.RawSamplePrefix = "" }, "s3.raw_sample_prefix"},
	} {
		c, err := Load("")
		if err != nil {
			t.Fatal(err)
		}
		tc.edit(&c.S3)
		if err := c.Validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v", tc.name, err)
		}
	}
	c, _ := Load("")
	c.S3.RawPrefix, c.S3.RawKeepPrefix, c.S3.RawSamplePrefix, c.S3.MediaPrefix = "igs/30d/", "igs/keep/failures/", "igs/keep/sample/", "igs/"
	if err := c.Validate(); err != nil {
		t.Errorf("the platform's prefixes: %v", err)
	}
}

// The platform's Valkey and Kafka present certificates signed by its own CA
// (DigitalOcean's project CA), handed over as PEM in VALKEY_CA_CERT and
// KAFKA_CA_CERT: igs trusts it, and a value that is no certificate stops
// it at start.
func TestPlatformCA(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "synthetic project CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	caCert, _ := x509.ParseCertificate(caDER)
	leafKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	leafDER, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{SerialNumber: big.NewInt(2),
		DNSNames: []string{"valkey.platform.svc", "k1.platform.svc"}, NotBefore: time.Now().Add(-time.Hour),
		NotAfter: time.Now().Add(time.Hour), ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}, caCert, &leafKey.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, _ := x509.ParseCertificate(leafDER)
	caPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}))

	env := map[string]string{PlatformEnv: "true", "POSTGRES_HOST": "pg", "POSTGRES_DATABASE": "igs", "POSTGRES_USER": "igs",
		"POSTGRES_PASSWORD": "x", "KAFKA_BROKERS": "k1.platform.svc:25073", "KAFKA_USER": "igs", "KAFKA_PASSWORD": "kpass", "KAFKA_SASL_MECHANISM": "SCRAM-SHA-256",
		"KAFKA_TLS": "true", "KAFKA_CA_CERT": caPEM, "VALKEY_HOST": "valkey.platform.svc", "VALKEY_PORT": "25061",
		"VALKEY_TLS": "true", "VALKEY_CA_CERT": caPEM, "POSTGRES_SSLMODE": "require", "POSTGRES_CA_CERT": caPEM, "S3_ENDPOINT": "s3:9000", "S3_ACCESS_KEY": "a", "S3_SECRET_KEY": "s",
		"S3_BUCKET_RAW": "r", "S3_BUCKET_MEDIA": "m"}
	for k, v := range env {
		t.Setenv(k, v)
	}
	c, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if c.Postgres.CAPEM != strings.TrimSpace(caPEM) {
		t.Error("POSTGRES_CA_CERT not read")
	}
	if c.Kafka.SASL.Mechanism != "scram-sha-256" {
		t.Errorf("kafka sasl %q, want scram-sha-256", c.Kafka.SASL.Mechanism)
	}
	for name, tc := range map[string]struct {
		tls  TLS
		host string
	}{"valkey": {c.Redis.TLS, "valkey.platform.svc"}, "kafka": {c.Kafka.TLS, "k1.platform.svc"}} {
		cfg, err := tc.tls.Config()
		if err != nil || cfg == nil {
			t.Fatalf("%s: tls %v", name, err)
		}
		if _, err := leaf.Verify(x509.VerifyOptions{Roots: cfg.RootCAs, DNSName: tc.host}); err != nil {
			t.Errorf("%s: the platform CA's certificate is not trusted: %v", name, err)
		}
	}
	if _, err := (TLS{Enabled: true, CAPEM: "not a certificate"}).Config(); err == nil {
		t.Error("a CA that is no PEM certificate was accepted")
	}
	// An empty POSTGRES_CA_CERT means none was given: no verification.
	t.Setenv("POSTGRES_CA_CERT", "")
	if c, err := Load(""); err != nil || c.Postgres.CAPEM != "" {
		t.Errorf("an empty POSTGRES_CA_CERT: %q, %v", c.Postgres.CAPEM, err)
	}
}
