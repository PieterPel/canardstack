// Response comparison, following oteldb's comparer: implementation-defined discovery labels are
// stripped, series are sorted by label set, numbers are compared approximately and NaN equals NaN.
package logqlcompat_test

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

const fraction = 0.00001

var discoveryLabels = []string{"service_name", "detected_level", "level"}

type envelope struct {
	Status    string `json:"status"`
	ErrorType string `json:"errorType"`
	Error     string `json:"error"`
	Data      struct {
		ResultType string          `json:"resultType"`
		Result     json.RawMessage `json:"result"`
	} `json:"data"`
}

type series struct {
	Labels map[string]string
	// Streams: [ts, line] pairs. Vectors and matrices: [t, value] pairs.
	Points [][2]string
}

func strip(labels map[string]string) map[string]string {
	out := make(map[string]string, len(labels))
	for k, v := range labels {
		out[k] = v
	}
	for _, k := range discoveryLabels {
		delete(out, k)
	}
	return out
}

func labelKey(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&sb, "%s=%q,", k, labels[k])
	}
	return sb.String()
}

// normalise turns a response into sorted series, or reports a result type it cannot read.
func normalise(e *envelope) (kind string, out []series, err error) {
	kind = e.Data.ResultType
	switch kind {
	case "streams":
		var raw []struct {
			Stream map[string]string `json:"stream"`
			Values [][]any           `json:"values"`
		}
		if err = json.Unmarshal(e.Data.Result, &raw); err != nil {
			return
		}
		for _, s := range raw {
			item := series{Labels: strip(s.Stream)}
			for _, v := range s.Values {
				item.Points = append(item.Points, [2]string{fmt.Sprint(v[0]), fmt.Sprint(v[1])})
			}
			out = append(out, item)
		}
	case "vector":
		var raw []struct {
			Metric map[string]string `json:"metric"`
			Value  []any             `json:"value"`
		}
		if err = json.Unmarshal(e.Data.Result, &raw); err != nil {
			return
		}
		for _, s := range raw {
			out = append(out, series{Labels: strip(s.Metric), Points: [][2]string{{fmt.Sprint(s.Value[0]), fmt.Sprint(s.Value[1])}}})
		}
	case "matrix":
		var raw []struct {
			Metric map[string]string `json:"metric"`
			Values [][]any           `json:"values"`
		}
		if err = json.Unmarshal(e.Data.Result, &raw); err != nil {
			return
		}
		for _, s := range raw {
			item := series{Labels: strip(s.Metric)}
			for _, v := range s.Values {
				item.Points = append(item.Points, [2]string{fmt.Sprint(v[0]), fmt.Sprint(v[1])})
			}
			out = append(out, item)
		}
	case "scalar":
		var raw []any
		if err = json.Unmarshal(e.Data.Result, &raw); err != nil {
			return
		}
		out = append(out, series{Points: [][2]string{{fmt.Sprint(raw[0]), fmt.Sprint(raw[1])}}})
	default:
		err = fmt.Errorf("unknown result type %q", kind)
	}
	sort.SliceStable(out, func(i, j int) bool { return labelKey(out[i].Labels) < labelKey(out[j].Labels) })
	return
}

func isEmpty(kind string, s []series) bool {
	if kind == "scalar" {
		return false
	}
	for _, item := range s {
		if len(item.Points) > 0 {
			return false
		}
	}
	return true
}

func sameNumber(a, b string) bool {
	x, errA := strconv.ParseFloat(a, 64)
	y, errB := strconv.ParseFloat(b, 64)
	if errA != nil || errB != nil {
		return false
	}
	switch {
	case math.IsNaN(x) && math.IsNaN(y):
		return true
	case x == y:
		return true
	}
	return math.Abs(x-y) <= fraction*math.Max(math.Abs(x), math.Abs(y))
}

// diff describes the first difference between the reference and the target, or "" if none.
func diff(kind string, ref, got []series) string {
	// Series that hold no points are equivalent to absent ones.
	nonEmpty := func(in []series) (out []series) {
		for _, s := range in {
			if len(s.Points) > 0 {
				out = append(out, s)
			}
		}
		return
	}
	ref, got = nonEmpty(ref), nonEmpty(got)
	if len(ref) != len(got) {
		return fmt.Sprintf("%d series, want %d\n  want: %s\n  got:  %s", len(got), len(ref), describe(ref), describe(got))
	}
	for i := range ref {
		if labelKey(ref[i].Labels) != labelKey(got[i].Labels) {
			return fmt.Sprintf("series %d has labels {%s}, want {%s}", i, labelKey(got[i].Labels), labelKey(ref[i].Labels))
		}
		if len(ref[i].Points) != len(got[i].Points) {
			return fmt.Sprintf("{%s}: %d points, want %d", labelKey(ref[i].Labels), len(got[i].Points), len(ref[i].Points))
		}
		for j, want := range ref[i].Points {
			have := got[i].Points[j]
			sameValue := want[1] == have[1]
			if kind != "streams" {
				sameValue = sameNumber(want[1], have[1])
			}
			if want[0] != have[0] || !sameValue {
				return fmt.Sprintf("{%s} point %d: got %v, want %v", labelKey(ref[i].Labels), j, have, want)
			}
		}
	}
	return ""
}

func describe(in []series) string {
	var parts []string
	for _, s := range in {
		parts = append(parts, fmt.Sprintf("{%s}x%d", labelKey(s.Labels), len(s.Points)))
		if len(parts) == 6 {
			parts = append(parts, "...")
			break
		}
	}
	return strings.Join(parts, " ")
}

func jsonUnmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
