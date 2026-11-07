// Latency recording for the comparison against the reference engine: with TIMING=<file> every
// request is repeated 4 times (the first round is cold) and each duration is appended to the
// file as a JSON line. Run with -parallel 1 so requests do not compete with each other.
package promqlcompat_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"time"
)

var (
	timingPath = os.Getenv("TIMING")
	timingMu   sync.Mutex
)

const timingRounds = 4

type timingRecord struct {
	Lang   string  `json:"lang"`
	Target string  `json:"target"`
	Query  string  `json:"query"`
	Round  int     `json:"round"`
	Ms     float64 `json:"ms"`
	OK     bool    `json:"ok"`
}

func recordTiming(lang, target, query string, round int, d time.Duration, ok bool) {
	line, _ := json.Marshal(timingRecord{lang, target, query, round, float64(d.Microseconds()) / 1000, ok})
	timingMu.Lock()
	defer timingMu.Unlock()
	if f, err := os.OpenFile(timingPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644); err == nil {
		f.Write(append(line, '\n'))
		f.Close()
	}
}

// measure runs fn once, or timingRounds times when timing is on. fn reports whether the request
// succeeded; the error is only for transport failures.
func measure(lang, target, query string, fn func() (bool, error)) error {
	if timingPath == "" {
		_, err := fn()
		return err
	}
	var err error
	for round := 0; round < timingRounds; round++ {
		start := time.Now()
		var ok bool
		ok, err = fn()
		recordTiming(lang, target, query, round, time.Since(start), ok && err == nil)
		if err != nil {
			return err
		}
	}
	return nil
}

// timingProxy times the requests that pass through next. All but the last round are answered into a
// recorder; the last answers the client.
func timingProxy(lang, target string, next http.Handler) http.Handler {
	if timingPath == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The suite posts queries as form bodies, so keep the body to replay it every round.
		body, _ := io.ReadAll(r.Body)
		key := r.URL.Path + "?" + r.URL.RawQuery
		if len(body) > 0 {
			key += "&" + string(body)
		}
		again := func() *http.Request {
			c := r.Clone(r.Context())
			c.Body = io.NopCloser(bytes.NewReader(body))
			return c
		}
		for round := 0; round < timingRounds; round++ {
			start := time.Now()
			if round < timingRounds-1 {
				rec := httptest.NewRecorder()
				next.ServeHTTP(rec, again())
				recordTiming(lang, target, key, round, time.Since(start), rec.Code == 200)
				continue
			}
			sw := &statusWriter{ResponseWriter: w, code: 200}
			next.ServeHTTP(sw, again())
			recordTiming(lang, target, key, round, time.Since(start), sw.code == 200)
		}
	})
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (s *statusWriter) WriteHeader(code int) {
	s.code = code
	s.ResponseWriter.WriteHeader(code)
}
