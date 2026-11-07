//! Binary operators: scalar and vector arithmetic, comparisons, vector matching, set operators.

use super::*;

impl Ev<'_> {
    pub(super) fn binary(&self, b: &parser::BinaryExpr) -> ApiResult<Value> {
        let op = b.op.to_string();
        let comparison = b.op.is_comparison_operator();
        let set_op = b.op.is_set_operator();
        let as_bool = b.modifier.as_ref().is_some_and(|m| m.return_bool);
        let lhs = self.eval(&b.lhs)?;
        let rhs = self.eval(&b.rhs)?;
        let n = self.n();
        match (lhs, rhs) {
            (Value::Scalar(a), Value::Scalar(c)) => {
                if comparison && !as_bool {
                    return Err(bad("comparisons between scalars must use the bool modifier"));
                }
                Ok(Value::Scalar(
                    (0..n)
                        .map(|i| match arith(&op, a[i], c[i]) {
                            Applied::Value(v) => v,
                            Applied::Cmp(t) => f64::from(t),
                        })
                        .collect(),
                ))
            }
            (Value::Vector(series), Value::Scalar(c)) => Ok(Value::Vector(
                self.vector_scalar(series, &c, &op, comparison, as_bool, false),
            )),
            (Value::Scalar(c), Value::Vector(series)) => Ok(Value::Vector(
                self.vector_scalar(series, &c, &op, comparison, as_bool, true),
            )),
            (Value::Vector(l), Value::Vector(r)) => {
                if set_op {
                    Ok(Value::Vector(self.set_op(&op, l, r, &b.modifier)))
                } else {
                    self.vector_vector(&op, comparison, as_bool, l, r, &b.modifier)
                        .map(Value::Vector)
                }
            }
            _ => Err(bad("binary operator on a string")),
        }
    }

    pub(super) fn vector_scalar(
        &self,
        series: Vec<Series>,
        scalar: &[f64],
        op: &str,
        comparison: bool,
        as_bool: bool,
        scalar_left: bool,
    ) -> Vec<Series> {
        let mut out = Vec::new();
        for mut s in series {
            if !comparison || as_bool {
                s.labels.remove(METRIC_NAME);
            }
            for (i, slot) in s.vals.iter_mut().enumerate() {
                let Some(v) = *slot else { continue };
                let (a, c) = if scalar_left {
                    (scalar[i], v)
                } else {
                    (v, scalar[i])
                };
                *slot = match arith(op, a, c) {
                    Applied::Value(x) => Some(x),
                    Applied::Cmp(t) if as_bool => Some(f64::from(t)),
                    Applied::Cmp(true) => Some(v),
                    Applied::Cmp(false) => None,
                };
            }
            if s.vals.iter().any(Option::is_some) {
                out.push(s);
            }
        }
        out
    }

    pub(super) fn signature(labels: &Labels, matching: &Option<LabelModifier>) -> Labels {
        let base = labels.iter().filter(|(k, _)| k.as_str() != METRIC_NAME);
        match matching {
            None => base.map(|(k, v)| (k.clone(), v.clone())).collect(),
            Some(LabelModifier::Include(l)) => {
                let keep: Vec<String> = l.labels.iter().map(|n| unescape(n)).collect();
                base.filter(|(k, _)| keep.contains(k))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            }
            Some(LabelModifier::Exclude(l)) => {
                let drop: Vec<String> = l.labels.iter().map(|n| unescape(n)).collect();
                base.filter(|(k, _)| !drop.contains(k))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            }
        }
    }

    pub(super) fn vector_vector(
        &self,
        op: &str,
        comparison: bool,
        as_bool: bool,
        lhs: Vec<Series>,
        rhs: Vec<Series>,
        modifier: &Option<parser::BinModifier>,
    ) -> ApiResult<Vec<Series>> {
        let matching = modifier.as_ref().and_then(|m| m.matching.clone());
        let card = modifier
            .as_ref()
            .map_or(VectorMatchCardinality::OneToOne, |m| m.card.clone());
        // `one` is the side that must match at most one series per signature.
        let (many, one, flipped) = match &card {
            VectorMatchCardinality::OneToMany(_) => (&rhs, &lhs, true),
            _ => (&lhs, &rhs, false),
        };
        let include: Vec<String> = match &card {
            VectorMatchCardinality::ManyToOne(l) | VectorMatchCardinality::OneToMany(l) => {
                l.labels.iter().map(|n| unescape(n)).collect()
            }
            _ => Vec::new(),
        };
        let mut acc = Acc::new(self.n());
        for step in 0..self.n() {
            let mut table: HashMap<Labels, usize> = HashMap::new();
            for (i, s) in one.iter().enumerate() {
                if s.vals[step].is_some()
                    && table
                        .insert(Self::signature(&s.labels, &matching), i)
                        .is_some()
                {
                    return Err(exec_err(
                        "found duplicate series for the match group on the right hand side; many-to-one matching must be explicit (group_left/group_right)",
                    ));
                }
            }
            let mut seen: HashMap<Labels, ()> = HashMap::new();
            for m in many {
                let Some(mv) = m.vals[step] else { continue };
                let sig = Self::signature(&m.labels, &matching);
                let Some(&oi) = table.get(&sig) else { continue };
                let ov = one[oi].vals[step].unwrap_or(f64::NAN);
                let (a, c) = if flipped { (ov, mv) } else { (mv, ov) };
                let value = match arith(op, a, c) {
                    Applied::Value(x) => x,
                    Applied::Cmp(t) if as_bool => f64::from(t),
                    Applied::Cmp(true) => a,
                    Applied::Cmp(false) => continue,
                };
                let mut labels = m.labels.clone();
                if !comparison || as_bool {
                    labels.remove(METRIC_NAME);
                }
                if matches!(card, VectorMatchCardinality::OneToOne) {
                    match &matching {
                        Some(LabelModifier::Include(_)) => labels.retain(|k, _| sig.contains_key(k)),
                        Some(LabelModifier::Exclude(l)) => {
                            for name in &l.labels {
                                labels.remove(&unescape(name));
                            }
                        }
                        None => {}
                    }
                } else {
                    for name in &include {
                        match one[oi].labels.get(name) {
                            Some(v) => labels.insert(name.clone(), v.clone()),
                            None => labels.remove(name),
                        };
                    }
                }
                if matches!(card, VectorMatchCardinality::OneToOne)
                    && seen.insert(labels.clone(), ()).is_some()
                {
                    return Err(exec_err(
                        "multiple matches for labels: grouping labels must ensure unique matches",
                    ));
                }
                acc.set(labels, step, value);
            }
        }
        Ok(acc.finish())
    }

    pub(super) fn set_op(
        &self,
        op: &str,
        lhs: Vec<Series>,
        rhs: Vec<Series>,
        modifier: &Option<parser::BinModifier>,
    ) -> Vec<Series> {
        let matching = modifier.as_ref().and_then(|m| m.matching.clone());
        let mut acc = Acc::new(self.n());
        for step in 0..self.n() {
            let rsigs: HashMap<Labels, ()> = rhs
                .iter()
                .filter(|s| s.vals[step].is_some())
                .map(|s| (Self::signature(&s.labels, &matching), ()))
                .collect();
            let mut lsigs: HashMap<Labels, ()> = HashMap::new();
            for s in &lhs {
                let Some(v) = s.vals[step] else { continue };
                let sig = Self::signature(&s.labels, &matching);
                lsigs.insert(sig.clone(), ());
                let hit = rsigs.contains_key(&sig);
                if (op == "and" && hit) || (op == "unless" && !hit) || op == "or" {
                    acc.set(s.labels.clone(), step, v);
                }
            }
            if op == "or" {
                for s in &rhs {
                    let Some(v) = s.vals[step] else { continue };
                    if !lsigs.contains_key(&Self::signature(&s.labels, &matching)) {
                        acc.set(s.labels.clone(), step, v);
                    }
                }
            }
        }
        acc.finish()
    }
}

/// Builds result series keyed by label set while evaluating step by step.
pub(super) struct Acc {
    steps: usize,
    index: HashMap<Labels, usize>,
    series: Vec<Series>,
}

impl Acc {
    pub(super) fn new(steps: usize) -> Self {
        Acc {
            steps,
            index: HashMap::new(),
            series: Vec::new(),
        }
    }

    pub(super) fn set(&mut self, labels: Labels, step: usize, value: f64) {
        let next = self.series.len();
        let idx = *self.index.entry(labels.clone()).or_insert(next);
        if idx == next {
            self.series.push(Series {
                labels,
                vals: vec![None; self.steps],
            });
        }
        let slot = &mut self.series[idx].vals[step];
        if slot.is_none() {
            *slot = Some(value);
        }
    }

    pub(super) fn finish(mut self) -> Vec<Series> {
        self.series.sort_by(|a, b| a.labels.cmp(&b.labels));
        self.series
    }
}

pub(super) enum Applied {
    Value(f64),
    Cmp(bool),
}

pub(super) fn arith(op: &str, a: f64, b: f64) -> Applied {
    match op {
        "+" => Applied::Value(a + b),
        "-" => Applied::Value(a - b),
        "*" => Applied::Value(a * b),
        "/" => Applied::Value(a / b),
        "%" => Applied::Value(a % b),
        "^" => Applied::Value(a.powf(b)),
        "atan2" => Applied::Value(a.atan2(b)),
        "==" => Applied::Cmp(a == b),
        "!=" => Applied::Cmp(a != b),
        ">" => Applied::Cmp(a > b),
        "<" => Applied::Cmp(a < b),
        ">=" => Applied::Cmp(a >= b),
        "<=" => Applied::Cmp(a <= b),
        _ => Applied::Value(f64::NAN),
    }
}
