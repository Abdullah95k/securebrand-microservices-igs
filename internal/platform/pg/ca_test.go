package pg

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// newCA makes a synthetic CA and a server certificate it signed, for a host
// name other than the one dialled (verify-ca checks the chain, not the
// name).
func newCA(t *testing.T, name string) (caPEM string, leafDER []byte) {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: name}, IsCA: true,
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign, NotBefore: time.Now().Add(-time.Hour),
		NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, _ := x509.ParseCertificate(der)
	leafKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	leafDER, err = x509.CreateCertificate(rand.Reader, &x509.Certificate{SerialNumber: big.NewInt(2),
		DNSNames: []string{"private-db.internal"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}, ca, &leafKey.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})), leafDER
}

// With the platform's CA (POSTGRES_CA_CERT), a TLS connection accepts only
// a server certificate that chains to it, whatever its host name
// (verify-ca). No CA changes nothing, a DSN without TLS stays without it,
// and a CA that is no certificate is refused.
func TestWithCA(t *testing.T) {
	caPEM, leaf := newCA(t, "synthetic project CA")
	_, otherLeaf := newCA(t, "another CA")
	parsed := func(mode string) *pgxpool.Config {
		t.Helper()
		c, err := pgxpool.ParseConfig("postgres://u:p@db.example:25060/igs?sslmode=" + mode)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}

	c := parsed("require")
	if err := WithCA(caPEM)(c); err != nil {
		t.Fatal(err)
	}
	tc := c.ConnConfig.TLSConfig
	if tc == nil || tc.VerifyConnection == nil {
		t.Fatal("sslmode require with a CA: no verification set")
	}
	peer := func(der []byte) tls.ConnectionState {
		c, err := x509.ParseCertificate(der)
		if err != nil {
			t.Fatal(err)
		}
		return tls.ConnectionState{PeerCertificates: []*x509.Certificate{c}}
	}
	if err := tc.VerifyConnection(peer(leaf)); err != nil {
		t.Errorf("the CA's server certificate refused: %v", err)
	}
	if err := tc.VerifyConnection(peer(otherLeaf)); err == nil {
		t.Error("a certificate from another CA accepted")
	}
	if err := tc.VerifyConnection(tls.ConnectionState{}); err == nil {
		t.Error("no certificate accepted")
	}

	// prefer: the TLS attempt verifies, the plain fallback stays plain.
	c = parsed("prefer")
	if err := WithCA(caPEM)(c); err != nil {
		t.Fatal(err)
	}
	if c.ConnConfig.TLSConfig == nil || c.ConnConfig.TLSConfig.VerifyConnection == nil {
		t.Error("sslmode prefer: the TLS attempt does not verify")
	}
	for _, f := range c.ConnConfig.Fallbacks {
		if f.TLSConfig != nil && f.TLSConfig.VerifyConnection == nil {
			t.Error("sslmode prefer: a TLS fallback does not verify")
		}
	}

	c = parsed("disable")
	if err := WithCA(caPEM)(c); err != nil || c.ConnConfig.TLSConfig != nil {
		t.Errorf("sslmode disable with a CA: TLS %v, %v", c.ConnConfig.TLSConfig, err)
	}
	c = parsed("require")
	if err := WithCA("  \n")(c); err != nil || c.ConnConfig.TLSConfig.VerifyConnection != nil {
		t.Errorf("an empty CA changed the TLS settings (%v)", err)
	}
	if err := WithCA("not a certificate")(parsed("require")); err == nil {
		t.Error("a CA that is no PEM certificate was accepted")
	}
}
