//! Prometheus native histograms (exponential buckets): the value type and the arithmetic PromQL
//! needs, ported from Prometheus' `model/histogram/float_histogram.go` and `promql/quantile.go`
//! so that results agree with it.
//!
//! A bucket with index `i` at schema `s` covers `(base^(i-1), base^i]` with `base = 2^(2^-s)`;
//! negative buckets mirror that. Buckets are kept in ordered maps of index to (float) count, which
//! makes merging and resolution changes plain map operations.

use super::exponential_bounds::EXPONENTIAL_BOUNDS;
use std::collections::BTreeMap;

pub const SCHEMA_MIN: i32 = -4;
pub const SCHEMA_MAX: i32 = 8;
/// The zero threshold Prometheus gives histograms converted from OpenTelemetry.
pub const DEFAULT_ZERO_THRESHOLD: f64 = 1e-128;

pub type Buckets = BTreeMap<i32, f64>;

#[derive(Clone, Debug)]
pub struct Hist {
    pub schema: i32,
    pub zero_threshold: f64,
    pub zero_count: f64,
    pub count: f64,
    pub sum: f64,
    pub pos: Buckets,
    pub neg: Buckets,
}

#[derive(Clone, Copy, Debug)]
pub struct Bucket {
    pub lower: f64,
    pub upper: f64,
    pub lower_inclusive: bool,
    pub upper_inclusive: bool,
    pub count: f64,
}

fn ldexp(mut x: f64, mut e: i32) -> f64 {
    while e > 1000 {
        x *= 2f64.powi(1000);
        e -= 1000;
    }
    while e < -1000 {
        x *= 2f64.powi(-1000);
        e += 1000;
    }
    x * 2f64.powi(e)
}

/// The upper bound of positive bucket `idx` at `schema`.
pub fn bound(idx: i32, schema: i32) -> f64 {
    if schema < 0 {
        let exp = (idx as i64) << (-schema);
        if exp == 1024 {
            return f64::MAX;
        }
        return ldexp(1.0, exp as i32);
    }
    let frac_idx = idx & ((1 << schema) - 1);
    let frac = EXPONENTIAL_BOUNDS[schema as usize][frac_idx as usize];
    let exp = (idx >> schema) + 1;
    if frac == 0.5 && exp == 1025 {
        return f64::MAX;
    }
    ldexp(frac, exp)
}

/// The index at the coarser `target` schema of a bucket of a finer schema.
fn target_idx(idx: i32, schema: i32, target: i32) -> i32 {
    ((idx - 1) >> (schema - target)) + 1
}

fn reduce_buckets(buckets: &Buckets, schema: i32, target: i32) -> Buckets {
    let mut out = Buckets::new();
    for (&idx, &count) in buckets {
        *out.entry(target_idx(idx, schema, target)).or_insert(0.0) += count;
    }
    out
}

impl Hist {
    pub fn empty(schema: i32) -> Hist {
        Hist { schema, zero_threshold: DEFAULT_ZERO_THRESHOLD, zero_count: 0.0, count: 0.0, sum: 0.0, pos: Buckets::new(), neg: Buckets::new() }
    }

    /// Converts an OpenTelemetry exponential histogram point the way Prometheus' OTLP translator
    /// does. A zero threshold that is not set (zero) becomes [`DEFAULT_ZERO_THRESHOLD`]; one that
    /// is set (from a scraped native histogram, say) is kept. `None` when the scale is below the
    /// smallest schema Prometheus has.
    #[allow(clippy::too_many_arguments)]
    pub fn from_otlp(
        scale: i32,
        zero_threshold: f64,
        zero_count: f64,
        count: f64,
        sum: f64,
        pos_offset: i32,
        pos: &[f64],
        neg_offset: i32,
        neg: &[f64],
    ) -> Option<Hist> {
        if scale < SCHEMA_MIN {
            return None;
        }
        let schema = scale.min(SCHEMA_MAX);
        let down = scale - schema;
        let layout = |offset: i32, counts: &[f64]| {
            let mut out = Buckets::new();
            for (i, &c) in counts.iter().enumerate() {
                if c != 0.0 {
                    *out.entry(((offset + i as i32) >> down) + 1).or_insert(0.0) += c;
                }
            }
            out
        };
        let zero_threshold = if zero_threshold > 0.0 { zero_threshold } else { DEFAULT_ZERO_THRESHOLD };
        Some(Hist { schema, zero_threshold, zero_count, count, sum, pos: layout(pos_offset, pos), neg: layout(neg_offset, neg) })
    }

    /// Merges buckets down to the coarser `target` schema.
    pub fn reduce_resolution(&mut self, target: i32) {
        if target >= self.schema {
            return;
        }
        self.pos = reduce_buckets(&self.pos, self.schema, target);
        self.neg = reduce_buckets(&self.neg, self.schema, target);
        self.schema = target;
    }

    pub fn at_schema(&self, target: i32) -> Hist {
        let mut h = self.clone();
        h.reduce_resolution(target);
        h
    }

    pub fn zero_bucket(&self) -> Bucket {
        Bucket { lower: -self.zero_threshold, upper: self.zero_threshold, lower_inclusive: true, upper_inclusive: true, count: self.zero_count }
    }

    /// All buckets in ascending order: negative ones from the most negative, the zero bucket (only
    /// when it holds a positive count), then the positive ones. A bucket that overlaps the zero
    /// bucket is cut at the zero threshold.
    pub fn all_buckets(&self) -> Vec<Bucket> {
        let threshold = self.zero_threshold;
        let mut out = Vec::with_capacity(self.neg.len() + self.pos.len() + 1);
        for (&idx, &count) in self.neg.iter().rev() {
            let mut b = Bucket { lower: -bound(idx, self.schema), upper: -bound(idx - 1, self.schema), lower_inclusive: true, upper_inclusive: false, count };
            if b.upper < 0.0 && b.upper > -threshold {
                b.upper = -threshold;
            }
            out.push(b);
        }
        if self.zero_count > 0.0 {
            out.push(self.zero_bucket());
        }
        for (&idx, &count) in &self.pos {
            let mut b = Bucket { lower: bound(idx - 1, self.schema), upper: bound(idx, self.schema), lower_inclusive: false, upper_inclusive: true, count };
            if b.lower > 0.0 && b.lower < threshold {
                b.lower = threshold;
            }
            out.push(b);
        }
        out
    }

    pub fn mul(&mut self, factor: f64) {
        self.zero_count *= factor;
        self.count *= factor;
        self.sum *= factor;
        self.pos.values_mut().for_each(|c| *c *= factor);
        self.neg.values_mut().for_each(|c| *c *= factor);
    }

    /// Dividing by zero sets everything to infinity and drops the buckets, as Prometheus does.
    pub fn div(&mut self, scalar: f64) {
        self.zero_count /= scalar;
        self.count /= scalar;
        self.sum /= scalar;
        if scalar == 0.0 {
            self.pos.clear();
            self.neg.clear();
            return;
        }
        self.pos.values_mut().for_each(|c| *c /= scalar);
        self.neg.values_mut().for_each(|c| *c /= scalar);
    }

    /// The zero count this histogram would have with a zero threshold of at least `larger`, and
    /// that threshold: it grows to the edge of a bucket the old one cuts through.
    fn zero_count_for_larger_threshold(&self, larger: f64) -> (f64, f64) {
        if larger == self.zero_threshold {
            return (self.zero_count, larger);
        }
        let mut larger = larger;
        'outer: loop {
            let mut zero = self.zero_count;
            for (&idx, &count) in &self.pos {
                let (lower, upper) = (bound(idx - 1, self.schema), bound(idx, self.schema));
                if lower >= larger {
                    break;
                }
                zero += count;
                if upper > larger {
                    if count != 0.0 {
                        larger = upper;
                    }
                    break;
                }
            }
            for (&idx, &count) in &self.neg {
                let (lower, upper) = (-bound(idx, self.schema), -bound(idx - 1, self.schema));
                if upper <= -larger {
                    break;
                }
                zero += count;
                if lower < -larger {
                    if count != 0.0 {
                        larger = -lower;
                        continue 'outer;
                    }
                    break;
                }
            }
            return (zero, larger);
        }
    }

    fn trim_buckets_in_zero_bucket(&mut self) {
        let (schema, threshold) = (self.schema, self.zero_threshold);
        self.pos.retain(|&idx, _| bound(idx - 1, schema) >= threshold);
        self.neg.retain(|&idx, _| -bound(idx - 1, schema) <= -threshold);
    }

    /// Makes both zero thresholds equal by widening the smaller one; returns the other
    /// histogram's zero count under that threshold.
    fn reconcile_zero_buckets(&mut self, other: &Hist) -> f64 {
        let mut other_zero = other.zero_count;
        let mut other_threshold = other.zero_threshold;
        while other_threshold != self.zero_threshold {
            if self.zero_threshold > other_threshold {
                (other_zero, other_threshold) = other.zero_count_for_larger_threshold(self.zero_threshold);
            }
            if other_threshold > self.zero_threshold {
                (self.zero_count, self.zero_threshold) = self.zero_count_for_larger_threshold(other_threshold);
                self.trim_buckets_in_zero_bucket();
            }
        }
        other_zero
    }

    fn combine(&mut self, other: &Hist, sign: f64) {
        let other_zero = self.reconcile_zero_buckets(other);
        self.zero_count += sign * other_zero;
        self.count += sign * other.count;
        self.sum += sign * other.sum;
        let reduced;
        let other = if other.schema < self.schema {
            self.reduce_resolution(other.schema);
            other
        } else if other.schema > self.schema {
            reduced = other.at_schema(self.schema);
            &reduced
        } else {
            other
        };
        let (schema, threshold) = (self.schema, self.zero_threshold);
        for (mine, theirs) in [(&mut self.pos, &other.pos), (&mut self.neg, &other.neg)] {
            // Buckets that fall inside the zero bucket were counted there.
            let mut below = true;
            for (&idx, &count) in theirs {
                if below && bound(idx, schema) <= threshold {
                    continue;
                }
                below = false;
                *mine.entry(idx).or_insert(0.0) += sign * count;
            }
            mine.retain(|_, c| *c != 0.0);
        }
    }

    pub fn add(&mut self, other: &Hist) {
        self.combine(other, 1.0);
    }

    pub fn sub(&mut self, other: &Hist) {
        self.combine(other, -1.0);
    }

    /// Whether `self`, a later sample of a counter, is lower than `previous` somewhere, which means
    /// the counter was reset in between.
    pub fn detect_reset(&self, previous: &Hist) -> bool {
        if self.count < previous.count || self.schema > previous.schema || self.zero_threshold < previous.zero_threshold {
            return true;
        }
        let (previous_zero, threshold) = previous.zero_count_for_larger_threshold(self.zero_threshold);
        if threshold != self.zero_threshold || self.zero_count < previous_zero {
            return true;
        }
        let dropped = |buckets: &Buckets| -> Buckets {
            // Leading buckets that lie within the zero bucket are not compared.
            let mut below = true;
            buckets
                .iter()
                .filter(|(&idx, _)| {
                    if below && bound(idx, self.schema) <= self.zero_threshold {
                        return false;
                    }
                    below = false;
                    true
                })
                .map(|(&i, &c)| (i, c))
                .collect()
        };
        for (current, earlier) in [(&self.pos, &previous.pos), (&self.neg, &previous.neg)] {
            let current = dropped(current);
            let earlier = dropped(&reduce_buckets(earlier, previous.schema, self.schema));
            if earlier.iter().any(|(idx, &count)| count != 0.0 && current.get(idx).is_none_or(|&c| c < count)) {
                return true;
            }
        }
        false
    }

    /// Same values (buckets with a zero count do not matter), as for `==` between histograms.
    pub fn equals(&self, other: &Hist) -> bool {
        let strip = |b: &Buckets| -> Vec<(i32, u64)> { b.iter().filter(|(_, &c)| c != 0.0).map(|(&i, &c)| (i, c.to_bits())).collect() };
        self.schema == other.schema
            && self.zero_threshold == other.zero_threshold
            && self.zero_count == other.zero_count
            && self.count == other.count
            && (self.sum == other.sum || (self.sum.is_nan() && other.sum.is_nan()))
            && strip(&self.pos) == strip(&other.pos)
            && strip(&self.neg) == strip(&other.neg)
    }

    /// The `q` quantile, interpolating inside the bucket it falls in (geometrically for
    /// exponential buckets, linearly in the zero bucket).
    pub fn quantile(&self, q: f64) -> f64 {
        if q < 0.0 {
            return f64::NEG_INFINITY;
        }
        if q > 1.0 {
            return f64::INFINITY;
        }
        if self.count == 0.0 || q.is_nan() {
            return f64::NAN;
        }
        let mut buckets = self.all_buckets();
        let forward = self.sum.is_nan() || q < 0.5;
        let mut rank = if forward { q * self.count } else { (1.0 - q) * self.count };
        if !forward {
            buckets.reverse();
        }
        let mut bucket = Bucket { lower: 0.0, upper: 0.0, lower_inclusive: false, upper_inclusive: false, count: 0.0 };
        let mut count = 0.0;
        for b in &buckets {
            bucket = *b;
            if bucket.count == 0.0 {
                continue;
            }
            count += bucket.count;
            if count >= rank {
                break;
            }
        }
        if bucket.lower < 0.0 && bucket.upper > 0.0 {
            if self.neg.is_empty() && !self.pos.is_empty() {
                bucket.lower = 0.0;
            } else if self.pos.is_empty() && !self.neg.is_empty() {
                bucket.upper = 0.0;
            }
        }
        if count > self.count {
            count = self.count;
        }
        if count < rank {
            return if self.sum.is_nan() { f64::NAN } else { bucket.upper };
        }
        if forward {
            rank -= count - bucket.count;
        } else {
            rank = count - rank;
        }
        let fraction = rank / bucket.count;
        if bucket.lower <= 0.0 && bucket.upper >= 0.0 {
            return bucket.lower + (bucket.upper - bucket.lower) * fraction;
        }
        let (log_lower, log_upper) = (bucket.lower.abs().log2(), bucket.upper.abs().log2());
        if bucket.lower > 0.0 {
            return (log_lower + (log_upper - log_lower) * fraction).exp2();
        }
        -(log_upper + (log_lower - log_upper) * (1.0 - fraction)).exp2()
    }

    /// The fraction of observations between `lower` and `upper`.
    pub fn fraction(&self, lower: f64, upper: f64) -> f64 {
        if self.count == 0.0 || lower.is_nan() || upper.is_nan() {
            return f64::NAN;
        }
        if lower >= upper {
            return 0.0;
        }
        let (mut count, mut rank, mut lower_rank, mut upper_rank) = (0.0, 0.0, 0.0, 0.0);
        let (mut lower_set, mut upper_set) = (false, false);
        let buckets = self.all_buckets();
        let mut consumed = 0;
        for mut b in buckets.iter().copied() {
            consumed += 1;
            count += b.count;
            let mut zero_bucket = false;
            if b.lower <= 0.0 && b.upper >= 0.0 {
                zero_bucket = true;
                if self.neg.is_empty() && !self.pos.is_empty() {
                    b.lower = 0.0;
                } else if self.pos.is_empty() && !self.neg.is_empty() {
                    b.upper = 0.0;
                }
            }
            if !lower_set && b.lower >= lower {
                lower_rank = rank;
                lower_set = true;
            }
            if !upper_set && b.lower >= upper {
                upper_rank = rank;
                upper_set = true;
            }
            if lower_set && upper_set {
                break;
            }
            let linear = |v: f64, rank: f64| {
                if b.lower == f64::NEG_INFINITY {
                    b.count
                } else {
                    rank + b.count * (v - b.lower) / (b.upper - b.lower)
                }
            };
            let exponential = |v: f64, rank: f64| {
                let (log_lower, log_upper, log_v) = (b.lower.abs().log2(), b.upper.abs().log2(), v.abs().log2());
                let fraction = if v > 0.0 { (log_v - log_lower) / (log_upper - log_lower) } else { 1.0 - ((log_v - log_upper) / (log_lower - log_upper)) };
                rank + b.count * fraction
            };
            if !lower_set && b.lower < lower && b.upper > lower {
                lower_rank = if zero_bucket { linear(lower, rank) } else { exponential(lower, rank) };
                lower_set = true;
            }
            if !upper_set && b.lower < upper && b.upper > upper {
                upper_rank = if zero_bucket { linear(upper, rank) } else { exponential(upper, rank) };
                upper_set = true;
            }
            if lower_set && upper_set {
                break;
            }
            rank += b.count;
        }
        if self.sum.is_nan() {
            count += buckets[consumed..].iter().map(|b| b.count).sum::<f64>();
        } else {
            count = self.count;
        }
        if !lower_set || lower_rank > count {
            lower_rank = count;
        }
        if !upper_set || upper_rank > count {
            upper_rank = count;
        }
        (upper_rank - lower_rank) / self.count
    }

    pub fn avg(&self) -> f64 {
        self.sum / self.count
    }

    /// The variance of the observations, each taken at its bucket's geometric mean.
    pub fn variance(&self) -> f64 {
        let mean = self.avg();
        let mut variance = 0.0;
        for b in self.all_buckets() {
            if b.count == 0.0 {
                continue;
            }
            let value = if b.lower <= 0.0 && b.upper >= 0.0 {
                0.0
            } else {
                let v = (b.upper * b.lower).sqrt();
                if b.upper < 0.0 {
                    -v
                } else {
                    v
                }
            };
            let delta = value - mean;
            variance += b.count * delta * delta;
        }
        variance / self.count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hist(schema: i32, zero: f64, pos: &[(i32, f64)], neg: &[(i32, f64)]) -> Hist {
        let count = zero + pos.iter().chain(neg).map(|b| b.1).sum::<f64>();
        Hist { schema, zero_threshold: DEFAULT_ZERO_THRESHOLD, zero_count: zero, count, sum: 0.0, pos: pos.iter().copied().collect(), neg: neg.iter().copied().collect() }
    }

    #[test]
    fn bounds_follow_the_schema() {
        assert_eq!(bound(0, 0), 1.0);
        assert_eq!(bound(1, 0), 2.0);
        assert_eq!(bound(-1, 0), 0.5);
        assert!((bound(1, 1) - 2f64.sqrt()).abs() < 1e-15);
        assert_eq!(bound(2, 1), 2.0);
        assert_eq!(bound(1, -1), 4.0);
        assert_eq!(bound(1, -2), 16.0);
        // Upper and lower bound of neighbours meet.
        for s in [-4, 0, 3, 8] {
            for i in [-7, 0, 5] {
                assert!(bound(i, s) < bound(i + 1, s));
            }
        }
    }

    #[test]
    fn otlp_buckets_shift_by_one_and_merge_when_the_scale_is_too_fine() {
        // OTLP bucket 0 covers (1, 2^(2^-s)]; Prometheus counts that as index 1.
        let h = Hist::from_otlp(0, 0.0, 1.0, 6.0, 1.0, 0, &[2.0, 3.0], 0, &[]).unwrap();
        assert_eq!(h.pos.iter().map(|(&i, &c)| (i, c)).collect::<Vec<_>>(), [(1, 2.0), (2, 3.0)]);
        // Scale 9 is reduced to schema 8: buckets 0 and 1 of scale 9 become index 1 of schema 8.
        let h = Hist::from_otlp(9, 0.0, 0.0, 5.0, 0.0, 0, &[2.0, 3.0], 0, &[]).unwrap();
        assert_eq!((h.schema, h.pos.iter().map(|(&i, &c)| (i, c)).collect::<Vec<_>>()), (8, vec![(1, 5.0)]));
        assert!(Hist::from_otlp(-5, 0.0, 0.0, 0.0, 0.0, 0, &[], 0, &[]).is_none());
        assert_eq!(Hist::from_otlp(0, 0.0, 0.0, 0.0, 0.0, 0, &[], 0, &[]).unwrap().zero_threshold, DEFAULT_ZERO_THRESHOLD);
        assert_eq!(Hist::from_otlp(0, 0.001, 0.0, 0.0, 0.0, 0, &[], 0, &[]).unwrap().zero_threshold, 0.001);
    }

    #[test]
    fn resolution_reduction_merges_neighbouring_buckets() {
        let mut h = hist(1, 0.0, &[(1, 1.0), (2, 2.0), (3, 4.0), (4, 8.0)], &[]);
        h.reduce_resolution(0);
        assert_eq!(h.pos.iter().map(|(&i, &c)| (i, c)).collect::<Vec<_>>(), [(1, 3.0), (2, 12.0)]);
        assert_eq!(h.schema, 0);
    }

    #[test]
    fn adding_and_subtracting_align_schema_and_zero_buckets() {
        let mut a = hist(1, 1.0, &[(1, 1.0), (2, 2.0)], &[]);
        let b = hist(0, 2.0, &[(1, 5.0)], &[(1, 1.0)]);
        a.add(&b);
        assert_eq!(a.schema, 0);
        assert_eq!(a.zero_count, 3.0);
        assert_eq!(a.pos.iter().map(|(&i, &c)| (i, c)).collect::<Vec<_>>(), [(1, 8.0)]);
        assert_eq!(a.neg.iter().map(|(&i, &c)| (i, c)).collect::<Vec<_>>(), [(1, 1.0)]);
        assert_eq!(a.count, 4.0 + 8.0);
        a.sub(&b);
        assert_eq!(a.zero_count, 1.0);
        assert_eq!(a.pos.iter().map(|(&i, &c)| (i, c)).collect::<Vec<_>>(), [(1, 3.0)]);
        // A wider zero threshold swallows the buckets it covers.
        let mut wide = hist(0, 0.0, &[(1, 4.0)], &[]);
        wide.zero_threshold = 2.0;
        let mut c = hist(0, 0.0, &[(0, 1.0), (1, 2.0), (2, 3.0)], &[]);
        c.add(&wide);
        assert_eq!(c.zero_threshold, 2.0);
        assert_eq!(c.zero_count, 1.0 + 2.0);
        assert_eq!(c.pos.iter().map(|(&i, &c)| (i, c)).collect::<Vec<_>>(), [(2, 3.0)]);
    }

    #[test]
    fn scaling_by_zero_drops_the_buckets() {
        let mut h = hist(0, 1.0, &[(1, 2.0)], &[]);
        h.mul(2.0);
        assert_eq!((h.zero_count, h.count, h.pos[&1]), (2.0, 6.0, 4.0));
        h.div(0.0);
        assert!(h.pos.is_empty() && h.count.is_infinite());
    }

    #[test]
    fn resets_are_found_in_counts_buckets_and_resolution() {
        let before = hist(0, 1.0, &[(1, 2.0), (2, 3.0)], &[]);
        let grown = hist(0, 1.0, &[(1, 2.0), (2, 4.0), (3, 1.0)], &[]);
        assert!(!grown.detect_reset(&before));
        let fewer = hist(0, 1.0, &[(1, 1.0), (2, 9.0)], &[]);
        assert!(fewer.detect_reset(&before)); // bucket 1 decreased although the count did not
        let lost_bucket = hist(0, 1.0, &[(2, 9.0)], &[]);
        assert!(lost_bucket.detect_reset(&before));
        let finer = hist(1, 1.0, &[(1, 2.0), (2, 4.0)], &[]);
        assert!(finer.detect_reset(&before)); // the schema went up
        let coarser = hist(-1, 1.0, &[(1, 9.0)], &[]);
        assert!(!coarser.detect_reset(&before));
    }

    #[test]
    fn quantiles_interpolate_geometrically_and_linearly_in_the_zero_bucket() {
        // 4 observations in (1, 2], 4 in (2, 4]; schema 0.
        let mut h = hist(0, 0.0, &[(1, 4.0), (2, 4.0)], &[]);
        h.sum = 20.0;
        assert_eq!(h.quantile(0.5), 2.0);
        assert!((h.quantile(0.25) - 2f64.powf(0.5)).abs() < 1e-12); // halfway through (1, 2] in log space
        assert!((h.quantile(0.75) - 2f64.powf(1.5)).abs() < 1e-12);
        assert_eq!(h.quantile(1.0), 4.0);
        assert_eq!(h.quantile(1.5), f64::INFINITY);
        assert_eq!(h.quantile(-0.1), f64::NEG_INFINITY);
        assert!(Hist::empty(0).quantile(0.5).is_nan());
        // Only positive buckets: the zero bucket's lower edge is taken as 0.
        let mut z = hist(0, 4.0, &[(1, 4.0)], &[]);
        z.sum = 6.0;
        assert!((z.quantile(0.25) - 0.5e-128).abs() < 1e-140);
    }

    #[test]
    fn fractions_and_statistics() {
        let mut h = hist(0, 0.0, &[(1, 4.0), (2, 4.0)], &[]);
        h.sum = 20.0;
        assert_eq!(h.fraction(f64::NEG_INFINITY, f64::INFINITY), 1.0);
        assert_eq!(h.fraction(1.0, 2.0), 0.5);
        assert_eq!(h.fraction(2.0, 1.0), 0.0);
        assert!((h.fraction(1.0, 2f64.powf(1.5)) - 0.75).abs() < 1e-12);
        assert_eq!(h.avg(), 20.0 / 8.0);
        // Observations at the geometric mean of their bucket: sqrt(2) and sqrt(8).
        let mean = 2.5;
        let expected = (4.0 * (2f64.sqrt() - mean).powi(2) + 4.0 * (8f64.sqrt() - mean).powi(2)) / 8.0;
        assert!((h.variance() - expected).abs() < 1e-12);
    }
}
