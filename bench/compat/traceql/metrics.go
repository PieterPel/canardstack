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

// normaliseCompare removes what Tempo's storage format adds to compare() answers: attributes in
// its dedicated columns get a series counting the spans that lack them (value "nil") and totals
// over every span. For those attributes the "nil" series and the totals are dropped on both sides.
func normaliseCompare(ref, got map[string]seriesSamples) {
	dedicated := map[string]bool{}
	for key := range ref {
		labels := strings.Split(key, ",")
		if len(labels) == 2 && strings.HasPrefix(labels[0], "__meta_type=") && !strings.HasSuffix(labels[0], "_total") && strings.HasSuffix(labels[1], "=nil") {
			dedicated[strings.TrimSuffix(labels[1], "=nil")] = true
		}
	}
	drop := func(m map[string]seriesSamples, nilToo bool) {
		for key := range m {
			labels := strings.Split(key, ",")
			if len(labels) != 2 || !strings.HasPrefix(labels[0], "__meta_type=") {
				continue
			}
			attr, value, _ := strings.Cut(labels[1], "=")
			if !dedicated[attr] {
				continue
			}
			if strings.HasSuffix(labels[0], "_total") || (nilToo && value == "nil") {
				delete(m, key)
			}
		}
	}
	drop(ref, true)
	drop(got, false)

	// Which of several equally frequent values makes the top N is arbitrary in Tempo (an unstable
	// sort), so when the chosen values differ but their counts are the same, the group is equal.
	group := func(m map[string]seriesSamples) map[string][]string {
		out := map[string][]string{}
		for key := range m {
			labels := strings.Split(key, ",")
			if len(labels) == 2 && strings.HasPrefix(labels[0], "__meta_type=") {
				attr, _, _ := strings.Cut(labels[1], "=")
				out[labels[0]+","+attr] = append(out[labels[0]+","+attr], key)
			}
		}
		return out
	}
	sums := func(m map[string]seriesSamples, keys []string) string {
		totals := make([]float64, 0, len(keys))
		for _, k := range keys {
			t := 0.0
			for _, v := range m[k] {
				t += v
			}
			totals = append(totals, t)
		}
		sort.Float64s(totals)
		return fmt.Sprint(totals)
	}
	refGroups, gotGroups := group(ref), group(got)
	for g, refKeys := range refGroups {
		gotKeys, ok := gotGroups[g]
		if !ok || strings.Join(sortedCopy(refKeys), "\n") == strings.Join(sortedCopy(gotKeys), "\n") {
			continue
		}
		if sums(ref, refKeys) == sums(got, gotKeys) {
			for _, k := range refKeys {
				delete(ref, k)
			}
			for _, k := range gotKeys {
				delete(got, k)
			}
		}
	}
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
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
		var missing, extra []string
		for _, k := range refKeys {
			if _, ok := got[k]; !ok {
				missing = append(missing, k)
			}
		}
		for _, k := range gotKeys {
			if _, ok := ref[k]; !ok {
				extra = append(extra, k)
			}
		}
		return fmt.Sprintf("series differ: missing %s; unexpected %s", preview(missing), preview(extra))
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
