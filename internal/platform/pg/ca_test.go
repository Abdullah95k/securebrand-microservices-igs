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
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// newCA makes a synthetic CA and a server certificate it signed for host.
func newCA(t *testing.T, name, host string) (caPEM string, server tls.Certificate) {
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
	leafDER, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{SerialNumber: big.NewInt(2), DNSNames: []string{host},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}, ca, &leafKey.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		tls.Certificate{Certificate: [][]byte{leafDER}, PrivateKey: leafKey}
}

// handshake runs a TLS handshake between client and a server presenting
// cert.
func handshake(t *testing.T, client *tls.Config, cert tls.Certificate) error {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(5 * time.Second))
		_ = tls.Server(c, &tls.Config{Certificates: []tls.Certificate{cert}}).Handshake()
	}()
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	return tls.Client(conn, client).Handshake()
}

// With the platform's roots (POSTGRES_CA_CERT), igs honours the DSN's
// sslmode: verify-full accepts only a server certificate from those roots
// for the host dialled; require (or verify-ca) accepts one from those roots
// whatever host it names. No CA changes nothing, a DSN without TLS stays
// without it, and a CA that is no certificate is refused.
func TestWithCA(t *testing.T) {
	caPEM, right := newCA(t, "synthetic root", "db.example")
	_, otherHost := newCA(t, "synthetic root 2", "elsewhere.example")
	rightRoot := func() tls.Certificate { return right }
	parsed := func(mode string) *pgxpool.Config {
		t.Helper()
		c, err := pgxpool.ParseConfig("postgres://u:p@db.example:25060/igs?sslmode=" + mode)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	with := func(mode, pem string) *tls.Config {
		t.Helper()
		c := parsed(mode)
		if err := WithCA(pem)(c); err != nil {
			t.Fatal(err)
		}
		return c.ConnConfig.TLSConfig
	}

	// verify-full: the right root and host pass; another root fails.
	full := with("verify-full", caPEM)
	if full == nil || full.InsecureSkipVerify {
		t.Fatal("verify-full with a CA does not verify")
	}
	if err := handshake(t, full, rightRoot()); err != nil {
		t.Errorf("verify-full: the root's certificate for the host refused: %v", err)
	}
	if err := handshake(t, full, otherHost); err == nil {
		t.Error("verify-full: a certificate from another root accepted")
	}
	// verify-full checks the host name: the same root, dialled by another
	// name, fails.
	full.ServerName = "other.example"
	if err := handshake(t, full, rightRoot()); err == nil {
		t.Error("verify-full: a certificate for another host accepted")
	}

	// require with a CA: the chain alone, whatever the host.
	req := with("require", caPEM)
	req.ServerName = "other.example"
	if err := handshake(t, req, rightRoot()); err != nil {
		t.Errorf("require: the root's certificate refused: %v", err)
	}
	if err := handshake(t, req, otherHost); err == nil {
		t.Error("require: a certificate from another root accepted")
	}
	if err := with("verify-ca", caPEM).VerifyConnection(tls.ConnectionState{}); err == nil {
		t.Error("verify-ca: no certificate accepted")
	}

	// prefer: the TLS attempt verifies, the plain fallback stays plain.
	c := parsed("prefer")
	if err := WithCA(caPEM)(c); err != nil {
		t.Fatal(err)
	}
	if c.ConnConfig.TLSConfig == nil || c.ConnConfig.TLSConfig.VerifyConnection == nil {
		t.Error("sslmode prefer: the TLS attempt does not verify")
	}
	if tc := with("disable", caPEM); tc != nil {
		t.Error("sslmode disable with a CA: TLS turned on")
	}
	c = parsed("require")
	if err := WithCA("  \n")(c); err != nil || c.ConnConfig.TLSConfig.VerifyConnection != nil || c.ConnConfig.TLSConfig.RootCAs != nil {
		t.Errorf("an empty CA changed the TLS settings (%v)", err)
	}
	if err := WithCA("not a certificate")(parsed("require")); err == nil {
		t.Error("a CA that is no PEM certificate was accepted")
	}
}
