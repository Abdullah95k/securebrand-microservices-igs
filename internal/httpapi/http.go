package httpapi

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
)

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(c int) { w.code = c; w.ResponseWriter.WriteHeader(c) }

func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// RequestLog counts requests by route and code and logs server errors.
func RequestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sw := &statusWriter{ResponseWriter: w, code: 200}
		start := time.Now()
		next.ServeHTTP(sw, r)
		route := ""
		if rc := chi.RouteContext(r.Context()); rc != nil {
			route = rc.RoutePattern()
		}
		metrics.APIRequests.WithLabelValues(route, strconv.Itoa(sw.code)).Inc()
		if sw.code >= 500 {
			slog.Error("api request", "method", r.Method, "route", route, "code", sw.code, "dur_ms", time.Since(start).Milliseconds())
		}
	})
}

// WriteJSON writes v as JSON with code.
func WriteJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// Problem writes an RFC 9457 problem document.
func Problem(w http.ResponseWriter, code int, title, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"type": "about:blank", "title": title, "status": code, "detail": detail})
}

// ServerError answers 404 for pgx.ErrNoRows and 500 otherwise.
func ServerError(w http.ResponseWriter, err error) {
	if errors.Is(err, pgx.ErrNoRows) {
		Problem(w, http.StatusNotFound, "not found", "no such resource")
		return
	}
	slog.Error("api error", "err", err)
	Problem(w, http.StatusInternalServerError, "internal error", "unexpected error")
}

// Decode reads a JSON body of at most 1 MiB with no unknown fields.
func Decode(r *http.Request, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

// LimitParam is the "limit" query parameter: 50 by default, at most 500.
func LimitParam(r *http.Request) int {
	n, err := strconv.Atoi(r.URL.Query().Get("limit"))
	if err != nil || n <= 0 {
		return 50
	}
	return min(n, 500)
}

// CursorPos is an opaque keyset position.
type CursorPos struct {
	T  time.Time `json:"t"`
	ID int64     `json:"i"`
}

// EncodeCursor encodes a keyset position.
func EncodeCursor(t time.Time, id int64) string {
	b, _ := json.Marshal(CursorPos{T: t, ID: id})
	return base64.RawURLEncoding.EncodeToString(b)
}

// DecodeCursor decodes a keyset position; false for none or a bad one.
func DecodeCursor(s string) (CursorPos, bool) {
	if s == "" {
		return CursorPos{}, false
	}
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return CursorPos{}, false
	}
	var c CursorPos
	if json.Unmarshal(b, &c) != nil {
		return CursorPos{}, false
	}
	return c, true
}

// PathInt is an integer path parameter.
func PathInt(r *http.Request, name string) (int64, bool) {
	v, err := strconv.ParseInt(chi.URLParam(r, name), 10, 64)
	return v, err == nil
}

// BoolParam is a boolean query parameter (1, true, yes).
func BoolParam(r *http.Request, name string) bool {
	v := strings.ToLower(r.URL.Query().Get(name))
	return v == "1" || v == "true" || v == "yes"
}
