// Search-response comparison: per trace, the root service and name, the trace duration and the
// set of spans the query matched. Order is not compared (Tempo returns the newest traces first).
package traceqlcompat_test

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

type searchSpan struct {
	SpanID string `json:"spanID"`
}

type spanSet struct {
	Spans []searchSpan `json:"spans"`
}

type searchTrace struct {
	TraceID         string    `json:"traceID"`
	RootServiceName string    `json:"rootServiceName"`
	RootTraceName   string    `json:"rootTraceName"`
	DurationMs      int       `json:"durationMs"`
	SpanSet         *spanSet  `json:"spanSet"`
	SpanSets        []spanSet `json:"spanSets"`
}

type searchResponse struct {
	Traces []searchTrace `json:"traces"`
}

type traceSummary struct {
	Root     string
	Duration int
	Spans    []string
}

func summarise(body []byte) (map[string]traceSummary, error) {
	var r searchResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, err
	}
	out := map[string]traceSummary{}
	for _, t := range r.Traces {
		sets := t.SpanSets
		if t.SpanSet != nil {
			sets = append(sets, *t.SpanSet)
		}
		seen := map[string]bool{}
		for _, s := range sets {
			for _, sp := range s.Spans {
				seen[sp.SpanID] = true
			}
		}
		spans := make([]string, 0, len(seen))
		for id := range seen {
			spans = append(spans, id)
		}
		sort.Strings(spans)
		out[t.TraceID] = traceSummary{Root: t.RootServiceName + " | " + t.RootTraceName, Duration: t.DurationMs, Spans: spans}
	}
	return out, nil
}

func keys(m map[string]traceSummary) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// diff describes the first difference between the reference and the target, or "".
func diff(ref, got map[string]traceSummary) string {
	if len(ref) != len(got) {
		var missing, extra []string
		for id := range ref {
			if _, ok := got[id]; !ok {
				missing = append(missing, id[:8])
			}
		}
		for id := range got {
			if _, ok := ref[id]; !ok {
				extra = append(extra, id[:8])
			}
		}
		sort.Strings(missing)
		sort.Strings(extra)
		return fmt.Sprintf("%d traces, want %d (missing %s; unexpected %s)", len(got), len(ref), preview(missing), preview(extra))
	}
	for _, id := range keys(ref) {
		want, have := ref[id], got[id]
		if _, ok := got[id]; !ok {
			return fmt.Sprintf("trace %s missing", id[:8])
		}
		if want.Root != have.Root {
			return fmt.Sprintf("trace %s root %q, want %q", id[:8], have.Root, want.Root)
		}
		if want.Duration != have.Duration {
			return fmt.Sprintf("trace %s duration %dms, want %dms", id[:8], have.Duration, want.Duration)
		}
		if strings.Join(want.Spans, ",") != strings.Join(have.Spans, ",") {
			return fmt.Sprintf("trace %s matched %d spans, want %d", id[:8], len(have.Spans), len(want.Spans))
		}
	}
	return ""
}

func preview(items []string) string {
	if len(items) == 0 {
		return "none"
	}
	if len(items) > 4 {
		return strings.Join(items[:4], ",") + ",..."
	}
	return strings.Join(items, ",")
}
