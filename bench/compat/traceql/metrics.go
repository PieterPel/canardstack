// Metrics-response comparison: per series (labels), the value at every step. A step without a
// value counts as 0 (the JSON encoding omits zeros), values compare to 1e-6 relative, and
// exemplars are ignored.
package traceqlcompat_test

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

type labelValue struct {
	String string   `json:"stringValue"`
	Int    string   `json:"intValue"`
	Float  *float64 `json:"doubleValue"`
	Bool   *bool    `json:"boolValue"`
}

func (v labelValue) text() string {
	switch {
	case v.Float != nil:
		return strconv.FormatFloat(*v.Float, 'g', -1, 64)
	case v.Bool != nil:
		return strconv.FormatBool(*v.Bool)
	case v.Int != "":
		return v.Int
	}
	return v.String
}

type metricsResponse struct {
	Series []struct {
		Labels []struct {
			Key   string     `json:"key"`
			Value labelValue `json:"value"`
		} `json:"labels"`
		Samples []struct {
			TimestampMs string  `json:"timestampMs"`
			Value       float64 `json:"value"`
		} `json:"samples"`
	} `json:"series"`
}

type seriesSamples map[string]float64

func summariseMetrics(body []byte) (map[string]seriesSamples, error) {
	var r metricsResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, err
	}
	out := map[string]seriesSamples{}
	for _, s := range r.Series {
		labels := make([]string, 0, len(s.Labels))
		for _, l := range s.Labels {
			labels = append(labels, l.Key+"="+l.Value.text())
		}
		sort.Strings(labels)
		samples := seriesSamples{}
		for _, p := range s.Samples {
			samples[p.TimestampMs] = p.Value
		}
		out[strings.Join(labels, ",")] = samples
	}
	return out, nil
}

func closeEnough(a, b float64) bool {
	if math.IsNaN(a) && math.IsNaN(b) {
		return true
	}
	return math.Abs(a-b) <= 1e-6*math.Max(1, math.Max(math.Abs(a), math.Abs(b)))
}

func diffMetrics(ref, got map[string]seriesSamples) string {
	refKeys, gotKeys := metricKeys(ref), metricKeys(got)
	if strings.Join(refKeys, "\n") != strings.Join(gotKeys, "\n") {
		return fmt.Sprintf("series differ: got %s, want %s", preview(gotKeys), preview(refKeys))
	}
	for _, key := range refKeys {
		want, have := ref[key], got[key]
		steps := map[string]bool{}
		for ts := range want {
			steps[ts] = true
		}
		for ts := range have {
			steps[ts] = true
		}
		for ts := range steps {
			if !closeEnough(want[ts], have[ts]) {
				return fmt.Sprintf("series {%s} at %s is %v, want %v", key, ts, have[ts], want[ts])
			}
		}
	}
	return ""
}

func metricKeys(m map[string]seriesSamples) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
