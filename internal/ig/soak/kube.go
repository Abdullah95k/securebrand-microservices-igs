package soak

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// Chaos is what the soak breaks on purpose.
type Chaos interface {
	// KillCrawler deletes one running crawler pod and returns its name.
	KillCrawler(ctx context.Context) (string, error)
	// RestartValkey restarts Valkey (its pods are recreated, its data lost).
	RestartValkey(ctx context.Context) error
}

// Kube does the soak's chaos through the Kubernetes API, as the pod's
// service account (deploy/soak: delete pods; get, list and patch Valkey's
// StatefulSet, in its own namespace only). It needs no client library: two
// REST calls and a patch.
type Kube struct {
	Base, Namespace, Token string
	// Crawlers selects the crawler pods (the chart's component labels).
	Crawlers string
	// Valkey is the StatefulSet restarted.
	Valkey string
	HTTP   *http.Client
	Rand   *rand.Rand
}

const saDir = "/var/run/secrets/kubernetes.io/serviceaccount/"

// InCluster reads the pod's service account.
func InCluster() (*Kube, error) {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, errors.New("soak: not in a Kubernetes pod (KUBERNETES_SERVICE_HOST unset)")
	}
	token, err := os.ReadFile(saDir + "token")
	if err != nil {
		return nil, fmt.Errorf("soak: service account token: %w", err)
	}
	ns, err := os.ReadFile(saDir + "namespace")
	if err != nil {
		return nil, fmt.Errorf("soak: service account namespace: %w", err)
	}
	ca, err := os.ReadFile(saDir + "ca.crt")
	if err != nil {
		return nil, fmt.Errorf("soak: cluster CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, errors.New("soak: the cluster CA holds no certificate")
	}
	return &Kube{Base: "https://" + net.JoinHostPort(host, port), Namespace: strings.TrimSpace(string(ns)),
		Token: strings.TrimSpace(string(token)),
		HTTP: &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{
			TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}}}, nil
}

func (k *Kube) do(ctx context.Context, method, path, contentType string, body io.Reader, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, k.Base+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+k.Token)
	req.Header.Set("Accept", "application/json")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := k.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("soak: %s %s: %d %s", method, path, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func (k *Kube) KillCrawler(ctx context.Context) (string, error) {
	var list struct {
		Items []struct {
			Metadata struct {
				Name              string  `json:"name"`
				DeletionTimestamp *string `json:"deletionTimestamp"`
			} `json:"metadata"`
			Status struct {
				Phase string `json:"phase"`
			} `json:"status"`
		} `json:"items"`
	}
	path := "/api/v1/namespaces/" + k.Namespace + "/pods?labelSelector=" + url.QueryEscape(k.Crawlers)
	if err := k.do(ctx, http.MethodGet, path, "", nil, &list); err != nil {
		return "", err
	}
	var running []string
	for _, p := range list.Items {
		if p.Status.Phase == "Running" && p.Metadata.DeletionTimestamp == nil {
			running = append(running, p.Metadata.Name)
		}
	}
	if len(running) == 0 {
		return "", errors.New("soak: no running crawler pod")
	}
	r := k.Rand
	if r == nil {
		r = rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 1))
	}
	name := running[r.IntN(len(running))]
	return name, k.do(ctx, http.MethodDelete, "/api/v1/namespaces/"+k.Namespace+"/pods/"+name, "", nil, nil)
}

func (k *Kube) RestartValkey(ctx context.Context) error {
	// What kubectl rollout restart does: a new pod-template annotation.
	patch := fmt.Sprintf(`{"spec":{"template":{"metadata":{"annotations":{"igs-soak/restarted-at":%q}}}}}`,
		time.Now().UTC().Format(time.RFC3339))
	return k.do(ctx, http.MethodPatch, "/apis/apps/v1/namespaces/"+k.Namespace+"/statefulsets/"+k.Valkey,
		"application/strategic-merge-patch+json", strings.NewReader(patch), nil)
}
