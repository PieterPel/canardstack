#!/usr/bin/env python3
"""Summarise the latency files the compat harnesses write (TIMING=<file>) as markdown.

    uv run --project bench bench/compat/latency_report.py bench/compat/results/latency-*.jsonl

Each line is one request: language, target, query, round (0 is cold), milliseconds, success. A query
is compared when both targets answered it successfully in every warm round; its latency is the
median of the warm rounds. The ratio is canardstack over the reference (above 1 is slower).
"""

import json
import re
import statistics
import sys
import urllib.parse
from collections import defaultdict
from pathlib import Path

REFERENCE = {"promql": "prometheus", "logql": "loki", "traceql": "tempo"}

FEATURES = {
    "promql": {
        "selector only": r"^[a-zA-Z_:][\w:]*(\{[^}]*\})?$",
        "rate/increase/irate": r"\b(rate|irate|increase)\(",
        "*_over_time": r"_over_time\(",
        "aggregation": r"\b(sum|avg|min|max|count|group|stddev|stdvar)\b\s*(by|without)?\s*\(",
        "topk/bottomk": r"\b(topk|bottomk)\(",
        "quantile": r"\bquantile\(",
        "histogram_quantile": r"histogram_quantile\(",
        "subquery": r"\[[^\]]*:[^\]]*\]",
        "vector matching": r"\b(on|ignoring|group_left|group_right)\b",
        "set operators": r"\b(and|or|unless)\b",
        "label_replace/join": r"label_(replace|join)\(",
        "offset / @": r"\boffset\b|@",
        "regex matcher": r"=~|!~",
        "time functions": r"\b(time|timestamp|day_of_week|hour|minute|month|year)\(",
    },
    "logql": {
        "stream selector only": r"^\{[^}]*\}$",
        "line filter": r"\|=|!=|\|~|!~",
        "| json": r"\|\s*json",
        "| logfmt": r"\|\s*logfmt",
        "| regexp": r"\|\s*regexp",
        "| pattern": r"\|\s*pattern",
        "| unpack": r"\|\s*unpack",
        "line_format / label_format": r"line_format|label_format",
        "label filter": r"\|\s*[a-z_]+\s*(=|!=|>|<|>=|<=|=~)\s*[\w\"]",
        "unwrap": r"unwrap",
        "rate/count_over_time": r"\b(rate|count_over_time|bytes_rate|bytes_over_time|rate_counter)\(",
        "*_over_time (unwrapped)": r"\b(sum|avg|min|max|stddev|stdvar|quantile|first|last)_over_time\(",
        "topk/bottomk": r"\b(topk|bottomk)\(",
        "vector aggregation": r"\b(sum|avg|min|max|count)\s*(by|without)?\s*\(",
        "binary op": r"\)\s*[-+*/]\s*|\s(and|or|unless)\s",
    },
    "traceql": {
        "empty / true filter": r"^\{\s*(true)?\s*\}$",
        "intrinsic filter": r"\b(duration|name|status|kind|statusMessage)\b",
        "attribute filter": r"\.\w|resource\.|span\.",
        "regex": r"=~|!~",
        "structural operator": r"(>>|<<|&>|&<|!>|!<|~)|\}\s*[<>]\s*\{",
        "pipeline aggregate": r"\|\s*(count|avg|min|max|sum)\(",
        "by()/coalesce()": r"\|\s*(by|coalesce)\(",
        "select()": r"select\(",
        "parent./event./link.": r"parent\.|event[.:]|link[.:]",
        "metrics: rate/count": r"\b(rate|count_over_time)\(\)",
        "metrics: *_over_time(attr)": r"\b(min|max|avg|sum)_over_time\(",
        "metrics: quantile/histogram": r"(quantile|histogram)_over_time\(",
        "metrics: topk/bottomk": r"\|\s*(topk|bottomk)\(",
        "metrics: compare": r"compare\(",
        "metrics: arithmetic": r"\)\s*[-+*/]\s|\s[-+*/]\s*\(",
    },
}


def query_text(lang: str, raw: str) -> str:
    """The query itself: for PromQL the request is a URL, and the query is its parameter."""
    if lang != "promql":
        return raw
    split = urllib.parse.urlsplit(raw)
    params = urllib.parse.parse_qs(split.query.replace("&", "&", 1))
    # A posted form is appended to the key after an "&".
    for part in raw.split("&"):
        for k, v in urllib.parse.parse_qsl(part):
            params.setdefault(k, []).append(v)
    return (params.get("query") or [raw])[0]


def load(paths):
    samples = defaultdict(lambda: defaultdict(dict))  # (lang, query) -> target -> round -> (ms, ok)
    for path in paths:
        for line in Path(path).read_text().splitlines():
            if not line.strip():
                continue
            r = json.loads(line)
            samples[(r["lang"], r["query"])][r["target"]][r["round"]] = (r["ms"], r["ok"])
    return samples


def summarise(samples):
    """(lang, query) -> dict with warm and cold milliseconds for both targets, for comparable ones."""
    out = {}
    for (lang, query), targets in samples.items():
        ref = REFERENCE[lang]
        if ref not in targets or "canardstack" not in targets:
            continue
        row = {}
        for name, key in ((ref, "ref"), ("canardstack", "got")):
            rounds = targets[name]
            warm = [ms for rnd, (ms, ok) in rounds.items() if rnd > 0 and ok]
            if not warm or len(warm) < len([r for r in rounds if r > 0]) or not rounds.get(0, (0, False))[1]:
                row = None
                break
            row[key] = statistics.median(warm)
            row[key + "_cold"] = rounds[0][0]
        if row:
            row["lang"], row["query"] = lang, query_text(lang, query)
            out[(lang, query)] = row
    return out


def pct(values, p):
    values = sorted(values)
    return values[min(len(values) - 1, int(round(p / 100 * (len(values) - 1))))]


def fmt(ms):
    return f"{ms:.1f}" if ms < 100 else f"{ms:.0f}"


def main():
    rows = summarise(load(sys.argv[1:]))
    if not rows:
        sys.exit("no comparable queries in the timing files")
    by_lang = defaultdict(list)
    for row in rows.values():
        by_lang[row["lang"]].append(row)

    print("# Latency: canardstack against the reference engines\n")
    print("Ratio is canardstack over the reference, from the median of the warm rounds (above 1 is slower).\n")
    print("| language | queries | reference total ms | canardstack total ms | ratio p50 | p90 | p99 | faster | ≤2x slower | 2–10x | >10x |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for lang, items in sorted(by_lang.items()):
        ratios = [r["got"] / max(r["ref"], 0.05) for r in items]
        print(
            f"| {lang} | {len(items)} | {fmt(sum(r['ref'] for r in items))} | {fmt(sum(r['got'] for r in items))} "
            f"| {pct(ratios, 50):.2f} | {pct(ratios, 90):.2f} | {pct(ratios, 99):.2f} "
            f"| {sum(x < 1 for x in ratios)} | {sum(1 <= x <= 2 for x in ratios)} | {sum(2 < x <= 10 for x in ratios)} | {sum(x > 10 for x in ratios)} |"
        )

    cold = defaultdict(list)
    for row in rows.values():
        cold[(row["lang"], "reference")].append(row["ref_cold"] / max(row["ref"], 0.05))
        cold[(row["lang"], "canardstack")].append(row["got_cold"] / max(row["got"], 0.05))
    print("\n## First request against later ones (median cold / warm)\n")
    print("| language | reference | canardstack |")
    print("| --- | --- | --- |")
    for lang in sorted(by_lang):
        print(f"| {lang} | {statistics.median(cold[(lang, 'reference')]):.2f} | {statistics.median(cold[(lang, 'canardstack')]):.2f} |")

    for lang, items in sorted(by_lang.items()):
        print(f"\n## {lang} by feature\n")
        print("| feature | queries | reference p50 ms | canardstack p50 ms | ratio p50 | ratio p90 | worst ratio |")
        print("| --- | --- | --- | --- | --- | --- | --- |")
        table = []
        for name, pattern in FEATURES[lang].items():
            rx = re.compile(pattern)
            hits = [r for r in items if rx.search(r["query"])]
            if len(hits) < 3:
                continue
            ratios = [r["got"] / max(r["ref"], 0.05) for r in hits]
            table.append((statistics.median(ratios), name, hits, ratios))
        for median, name, hits, ratios in sorted(table, reverse=True):
            print(
                f"| {name} | {len(hits)} | {fmt(statistics.median(r['ref'] for r in hits))} | {fmt(statistics.median(r['got'] for r in hits))} "
                f"| {median:.2f} | {pct(ratios, 90):.2f} | {max(ratios):.1f} |"
            )

        print(f"\n### {lang}: slowest relative to the reference\n")
        print("| ratio | reference ms | canardstack ms | query |")
        print("| --- | --- | --- | --- |")
        worst = sorted(items, key=lambda r: r["got"] / max(r["ref"], 0.05), reverse=True)[:12]
        for r in worst:
            q = " ".join(r["query"].split())
            print(f"| {r['got'] / max(r['ref'], 0.05):.1f} | {fmt(r['ref'])} | {fmt(r['got'])} | `{q[:110]}` |")

        print(f"\n### {lang}: slowest in absolute terms on canardstack\n")
        print("| canardstack ms | reference ms | query |")
        print("| --- | --- | --- |")
        for r in sorted(items, key=lambda r: r["got"], reverse=True)[:6]:
            q = " ".join(r["query"].split())
            print(f"| {fmt(r['got'])} | {fmt(r['ref'])} | `{q[:110]}` |")


if __name__ == "__main__":
    main()
