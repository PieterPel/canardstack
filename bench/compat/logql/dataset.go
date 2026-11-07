// A deterministic log dataset shaped like oteldb's generator output: streams labelled
// {filename, job="varlogs"} holding JSON lines with HTTP fields.
package logqlcompat_test

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/netip"
	"time"
)

type logLine struct {
	TS     time.Time
	Stream string // the `filename` label
	Line   string
}

func humanizeBytes(n uint64) string {
	if n < 1000 {
		return fmt.Sprintf("%d B", n)
	}
	units := []string{"kB", "MB", "GB", "TB"}
	value, exp := float64(n), -1
	for value >= 1000 && exp < len(units)-1 {
		value /= 1000
		exp++
	}
	format := "%.0f %s"
	if value < 10 {
		format = "%.1f %s"
	}
	return fmt.Sprintf(format, value, units[exp])
}

func pick[T any](r *rand.Rand, items []T) T { return items[r.Intn(len(items))] }

func generate(start, end time.Time, step time.Duration, perStep int, streams []string) []logLine {
	r := rand.New(rand.NewSource(1))
	levels := []string{"Trace", "Debug", "Info", "Warn", "Error", "Fatal"}
	protocols := []string{"HTTP/1.0", "HTTP/1.1", "HTTP/2.0"}
	methods := []string{"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"}
	statuses := []int{200, 201, 204, 400, 401, 403, 404, 405, 406, 415, 500, 501}
	var out []logLine
	for ts := start; ts.Before(end); ts = ts.Add(step) {
		rt := ts
		for i := 0; i < perStep; i++ {
			rt = rt.Add(100 * time.Microsecond)
			var client [4]byte
			r.Read(client[:])
			var span [8]byte
			var trace [16]byte
			r.Read(span[:])
			r.Read(trace[:])
			line, _ := json.Marshal(struct {
				TS         string `json:"ts"`
				Level      string `json:"level"`
				Protocol   string `json:"protocol"`
				Method     string `json:"method"`
				ClientIP   string `json:"client_ip"`
				RemoteIP   string `json:"remote_ip"`
				RemotePort int    `json:"remote_port"`
				Status     int    `json:"status"`
				Took       string `json:"took"`
				Size       string `json:"size"`
				SpanID     string `json:"span_id"`
				TraceID    string `json:"trace_id"`
			}{
				TS:         rt.UTC().Format(time.RFC3339Nano),
				Level:      pick(r, levels),
				Protocol:   pick(r, protocols),
				Method:     pick(r, methods),
				ClientIP:   netip.AddrFrom4(client).String(),
				RemoteIP:   "127.0.0.1",
				RemotePort: r.Intn(65535),
				Status:     pick(r, statuses),
				Took:       (time.Duration(r.Int63n(int64(30*time.Minute))) + time.Millisecond).String(),
				Size:       humanizeBytes(uint64(r.Intn(1024*1024)) + 1025),
				SpanID:     hex.EncodeToString(span[:]),
				TraceID:    hex.EncodeToString(trace[:]),
			})
			out = append(out, logLine{TS: rt, Stream: pick(r, streams), Line: string(line)})
		}
	}
	return out
}

// lokiPush is the body of POST /loki/api/v1/push.
func lokiPush(lines []logLine) []byte {
	type stream struct {
		Stream map[string]string `json:"stream"`
		Values [][2]string       `json:"values"`
	}
	byName := map[string]*stream{}
	var order []string
	for _, l := range lines {
		s, ok := byName[l.Stream]
		if !ok {
			s = &stream{Stream: map[string]string{"filename": l.Stream, "job": "varlogs"}}
			byName[l.Stream] = s
			order = append(order, l.Stream)
		}
		s.Values = append(s.Values, [2]string{fmt.Sprint(l.TS.UnixNano()), l.Line})
	}
	var streams []*stream
	for _, name := range order {
		streams = append(streams, byName[name])
	}
	body, _ := json.Marshal(map[string]any{"streams": streams})
	return body
}

// otlpLogs renders the lines as OTLP/JSON export requests, one per line of the result, with the
// stream labels as resource attributes.
func otlpLogs(lines []logLine) []string {
	const chunk = 200
	type attr struct {
		Key   string         `json:"key"`
		Value map[string]any `json:"value"`
	}
	str := func(v string) map[string]any { return map[string]any{"stringValue": v} }
	byStream := map[string][]logLine{}
	var order []string
	for _, l := range lines {
		if _, ok := byStream[l.Stream]; !ok {
			order = append(order, l.Stream)
		}
		byStream[l.Stream] = append(byStream[l.Stream], l)
	}
	var out []string
	for _, name := range order {
		all := byStream[name]
		for from := 0; from < len(all); from += chunk {
			to := min(from+chunk, len(all))
			var records []map[string]any
			for _, l := range all[from:to] {
				ns := fmt.Sprint(l.TS.UnixNano())
				records = append(records, map[string]any{"timeUnixNano": ns, "observedTimeUnixNano": ns, "body": str(l.Line)})
			}
			request, _ := json.Marshal(map[string]any{"resourceLogs": []any{map[string]any{
				"resource":  map[string]any{"attributes": []attr{{"filename", str(name)}, {"job", str("varlogs")}}},
				"scopeLogs": []any{map[string]any{"scope": map[string]any{"name": ""}, "logRecords": records}},
			}}})
			out = append(out, string(request))
		}
	}
	return out
}
