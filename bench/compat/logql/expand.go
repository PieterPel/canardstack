// Query expansion ported from oteldb's lokicompliance package (Apache-2.0,
// github.com/oteldb/oteldb/internal/lokicompliance).
package logqlcompat_test

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"text/template"
	"text/template/parse"
)

var variantArgs = map[string][]string{
	"range":    {"1s", "15s", "1m", "5m", "15m", "1h"},
	"offset":   {"1s", "5s", "15s"},
	"topN":     {"1", "2", "5", "10"},
	"quantile": {"0.1", "0.5", "0.75", "0.95", "0.90", "0.99", "1", "1.5"},

	"simpleRangeAggOp": {"count_over_time", "rate", "bytes_over_time", "bytes_rate"},
	"unwrapRangeAggOp": {
		"rate_counter", "avg_over_time", "sum_over_time", "min_over_time", "max_over_time",
		"stdvar_over_time", "stddev_over_time", "first_over_time", "last_over_time",
	},
	"groupRangeAggOp": {
		"avg_over_time", "stddev_over_time", "stdvar_over_time", "max_over_time", "min_over_time",
		"first_over_time", "last_over_time",
	},
	"unwrapExpr":        {"unwrap status", "unwrap duration(took)", "unwrap bytes(size)"},
	"unwrapLabels":      {"status,took,size"},
	"simpleVecAggOp":    {"sum", "avg", "max", "min", "count", "stddev", "stdvar"},
	"sortVecAggOp":      {"sort", "sort_desc"},
	"topBottomVecAggOp": {"topk", "bottomk"},

	"lineFilterOp": {"|=", "!=", "~=", "!~"},
	"arithBinOp":   {"+", "-", "*", "/", "%", "^"},
	"cmpBinOp":     {"==", "!=", ">", ">=", "<", "<="},
	"setBinOp":     {"and", "or", "unless"},
}

type testCase struct {
	Query         string
	ShouldFail    bool
	ShouldBeEmpty bool
	Skip          bool
}

type pattern struct {
	Query          string `yaml:"query"`
	SkipComparison bool   `yaml:"skip_comparison"`
	ShouldFail     bool   `yaml:"should_fail"`
	ShouldBeEmpty  bool   `yaml:"should_be_empty"`
}

func expand(patterns []pattern) ([]testCase, error) {
	var out []testCase
	for _, p := range patterns {
		t, err := template.New("q").Funcs(template.FuncMap{"quote": strconv.Quote}).Parse(p.Query)
		if err != nil {
			return nil, fmt.Errorf("parse %q: %w", p.Query, err)
		}
		args := collectArgs(t)
		var expandErr error
		add := func(query string) {
			out = append(out, testCase{Query: query, ShouldFail: p.ShouldFail, ShouldBeEmpty: p.ShouldBeEmpty, Skip: p.SkipComparison})
		}
		walk(t, args, map[string]string{}, add, &expandErr)
		if expandErr != nil {
			return nil, expandErr
		}
	}
	return out, nil
}

func walk(t *template.Template, rest []string, values map[string]string, add func(string), failed *error) {
	if len(rest) == 0 {
		var sb strings.Builder
		if err := t.Execute(&sb, values); err != nil {
			*failed = err
			return
		}
		add(sb.String())
		return
	}
	for _, v := range variantArgs[rest[0]] {
		values[rest[0]] = v
		walk(t, rest[1:], values, add, failed)
	}
}

func collectArgs(t *template.Template) []string {
	unique := map[string]struct{}{}
	visit(t.Root, func(n parse.Node) {
		if f, ok := n.(*parse.FieldNode); ok && len(f.Ident) == 1 {
			if _, known := variantArgs[f.Ident[0]]; known {
				unique[f.Ident[0]] = struct{}{}
			}
		}
	})
	args := slices.Collect(maps.Keys(unique))
	slices.Sort(args)
	return args
}

func visit(n parse.Node, cb func(parse.Node)) {
	if n == nil {
		return
	}
	cb(n)
	switch n := n.(type) {
	case *parse.ListNode:
		if n != nil {
			for _, sub := range n.Nodes {
				visit(sub, cb)
			}
		}
	case *parse.PipeNode:
		if n != nil {
			for _, sub := range n.Cmds {
				visit(sub, cb)
			}
		}
	case *parse.ActionNode:
		visit(n.Pipe, cb)
	case *parse.CommandNode:
		for _, sub := range n.Args {
			visit(sub, cb)
		}
	case *parse.IfNode:
		visit(&n.BranchNode, cb)
	case *parse.RangeNode:
		visit(&n.BranchNode, cb)
	case *parse.WithNode:
		visit(&n.BranchNode, cb)
	case *parse.BranchNode:
		visit(n.Pipe, cb)
		visit(n.List, cb)
		visit(n.ElseList, cb)
	}
}
