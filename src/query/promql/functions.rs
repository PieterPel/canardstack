//! Function calls and their numeric helpers.

use super::*;

/// Label names given to `label_replace` / `label_join` must be non-empty.
fn valid_label_names(names: &[&str]) -> ApiResult<()> {
    match names.iter().find(|n| n.is_empty()) {
        Some(_) => Err(bad("invalid label name: it must not be empty")),
        None => Ok(()),
    }
}

impl Ev<'_> {
    /// `absent()` / `absent_over_time()`: one series with value 1 at every step where `input` has
    /// no samples. Labels come from the equality matchers of the selector.
    pub(super) fn absent(&self, selector: &Expr, input: &[Series]) -> Value {
        let mut labels = Labels::new();
        if let Expr::VectorSelector(vs) = selector {
            for m in &vs.matchers.matchers {
                if matches!(m.op, MatchOp::Equal) && m.name != METRIC_NAME {
                    labels.insert(unescape(&m.name), m.value.clone());
                }
            }
        }
        let vals = (0..self.n())
            .map(|i| input.iter().all(|s| s.vals[i].is_none()).then_some(1.0))
            .collect();
        Value::Vector(vec![Series { labels, vals }])
    }

    pub(super) fn call_floats(&self, name: &str, args: &[Box<Expr>]) -> ApiResult<Value> {
        let arg = |i: usize| -> ApiResult<&Expr> {
            args.get(i)
                .map(|b| b.as_ref())
                .ok_or_else(|| bad(format!("{name}: missing argument")))
        };
        match name {
            "rate" | "increase" | "delta" => {
                let (counter, rate) = (name != "delta", name == "rate");
                self.windowed(arg(0)?, false, &|s, start, end| {
                    extrapolated(s, start, end, counter, rate)
                })
            }
            "irate" | "idelta" => {
                let counter = name == "irate";
                self.windowed(arg(0)?, false, &|s, _, _| {
                    let [.., prev, last] = s else { return None };
                    let dt = (last.0 - prev.0) as f64 / 1000.0;
                    let mut diff = last.1 - prev.1;
                    if counter && last.1 < prev.1 {
                        diff = last.1;
                    }
                    Some(if counter { diff / dt } else { diff })
                })
            }
            "changes" => self.windowed(arg(0)?, false, &|s, _, _| {
                Some(s.windows(2).filter(|w| w[0].1 != w[1].1).count() as f64)
            }),
            "resets" => self.windowed(arg(0)?, false, &|s, _, _| {
                Some(s.windows(2).filter(|w| w[1].1 < w[0].1).count() as f64)
            }),
            "avg_over_time" => self.windowed(arg(0)?, false, &|s, _, _| {
                Some(s.iter().map(|x| x.1).sum::<f64>() / s.len() as f64)
            }),
            "sum_over_time" => self.windowed(arg(0)?, false, &|s, _, _| {
                Some(s.iter().map(|x| x.1).sum::<f64>())
            }),
            "count_over_time" => {
                self.windowed(arg(0)?, false, &|s, _, _| Some(s.len() as f64))
            }
            "present_over_time" => self.windowed(arg(0)?, false, &|_, _, _| Some(1.0)),
            "min_over_time" => self.windowed(arg(0)?, false, &|s, _, _| {
                Some(s.iter().map(|x| x.1).fold(f64::INFINITY, f64::min))
            }),
            "max_over_time" => self.windowed(arg(0)?, false, &|s, _, _| {
                Some(s.iter().map(|x| x.1).fold(f64::NEG_INFINITY, f64::max))
            }),
            "last_over_time" => {
                self.windowed(arg(0)?, true, &|s, _, _| s.last().map(|x| x.1))
            }
            "stddev_over_time" | "stdvar_over_time" => {
                let sqrt = name == "stddev_over_time";
                self.windowed(arg(0)?, false, &|s, _, _| {
                    let var = variance(s.iter().map(|x| x.1));
                    Some(if sqrt { var.sqrt() } else { var })
                })
            }
            "quantile_over_time" => {
                let q = self.scalar_arg(arg(0)?)?.first().copied().unwrap_or(f64::NAN);
                self.windowed(arg(1)?, false, &|s, _, _| {
                    Some(quantile(q, s.iter().map(|x| x.1).collect()))
                })
            }
            "abs" => self.map_vector(arg(0)?, f64::abs),
            "ceil" => self.map_vector(arg(0)?, f64::ceil),
            "floor" => self.map_vector(arg(0)?, f64::floor),
            "sqrt" => self.map_vector(arg(0)?, f64::sqrt),
            "exp" => self.map_vector(arg(0)?, f64::exp),
            "ln" => self.map_vector(arg(0)?, f64::ln),
            "log2" => self.map_vector(arg(0)?, f64::log2),
            "log10" => self.map_vector(arg(0)?, f64::log10),
            "sgn" => self.map_vector(arg(0)?, |x| {
                if x.is_nan() || x == 0.0 {
                    x
                } else {
                    x.signum()
                }
            }),
            "round" => {
                let to = match args.get(1) {
                    Some(e) => self.scalar_arg(e)?.first().copied().unwrap_or(1.0),
                    None => 1.0,
                };
                self.map_vector(arg(0)?, |x| (x / to + 0.5).floor() * to)
            }
            "clamp" | "clamp_min" | "clamp_max" => {
                let (lo, hi) = match name {
                    "clamp" => (
                        self.scalar_arg(arg(1)?)?.first().copied().unwrap_or(f64::NAN),
                        self.scalar_arg(arg(2)?)?.first().copied().unwrap_or(f64::NAN),
                    ),
                    "clamp_min" => (
                        self.scalar_arg(arg(1)?)?.first().copied().unwrap_or(f64::NAN),
                        f64::INFINITY,
                    ),
                    _ => (
                        f64::NEG_INFINITY,
                        self.scalar_arg(arg(1)?)?.first().copied().unwrap_or(f64::NAN),
                    ),
                };
                if lo > hi {
                    // Prometheus returns nothing when the bounds are inverted.
                    return Ok(Value::Vector(Vec::new()));
                }
                self.map_vector(arg(0)?, |x| x.max(lo).min(hi))
            }
            "absent" => {
                let input = self.vector_arg(arg(0)?)?;
                Ok(self.absent(arg(0)?, &input))
            }
            "absent_over_time" => {
                let Value::Vector(input) = self.windowed(arg(0)?, false, &|_, _, _| Some(1.0))?
                else {
                    unreachable!("windowed returns a vector")
                };
                let selector = match arg(0)? {
                    Expr::MatrixSelector(ms) => Expr::VectorSelector(ms.vs.clone()),
                    other => other.clone(),
                };
                Ok(self.absent(&selector, &input))
            }
            "year" | "month" | "day_of_month" | "day_of_week" | "day_of_year" | "days_in_month"
            | "hour" | "minute" => self.datetime(name, args),
            "timestamp" => self.timestamp(arg(0)?),
            "deriv" | "predict_linear" => self.regression(name, args),
            "histogram_quantile" => self.histogram_quantile(args),
            "vector" => {
                let vals = self.scalar_arg(arg(0)?)?.into_iter().map(Some).collect();
                Ok(Value::Vector(vec![Series {
                    labels: Labels::new(),
                    vals,
                }]))
            }
            "scalar" => {
                let input = self.vector_arg(arg(0)?)?;
                Ok(Value::Scalar(
                    (0..self.n())
                        .map(|i| {
                            let mut present = input.iter().filter_map(|s| s.vals[i]);
                            match (present.next(), present.next()) {
                                (Some(v), None) => v,
                                _ => f64::NAN,
                            }
                        })
                        .collect(),
                ))
            }
            "time" => Ok(Value::Scalar(
                (0..self.n())
                    .map(|i| self.grid.ts(i) as f64 / 1000.0)
                    .collect(),
            )),
            "pi" => Ok(Value::Scalar(vec![std::f64::consts::PI; self.n()])),
            "sort" | "sort_desc" => {
                let mut series = self.vector_arg(arg(0)?)?;
                let key = |s: &Series| s.vals.last().copied().flatten().unwrap_or(f64::NAN);
                series.sort_by(|a, b| key(a).total_cmp(&key(b)));
                if name == "sort_desc" {
                    series.reverse();
                }
                Ok(Value::Vector(series))
            }
            "label_replace" => {
                let mut series = self.vector_arg(arg(0)?)?;
                let dst = self.string_arg(arg(1)?)?;
                let repl = self.string_arg(arg(2)?)?;
                let src = self.string_arg(arg(3)?)?;
                valid_label_names(&[&dst, &src])?;
                let rx = anchored(&self.string_arg(arg(4)?)?)?;
                for s in &mut series {
                    let value = s.labels.get(&src).cloned().unwrap_or_default();
                    if let Some(caps) = rx.captures(&value) {
                        let mut expanded = String::new();
                        caps.expand(&repl, &mut expanded);
                        if expanded.is_empty() {
                            s.labels.remove(&dst);
                        } else {
                            s.labels.insert(dst.clone(), expanded);
                        }
                    }
                }
                Ok(Value::Vector(series))
            }
            "label_join" => {
                let mut series = self.vector_arg(arg(0)?)?;
                let dst = self.string_arg(arg(1)?)?;
                let sep = self.string_arg(arg(2)?)?;
                valid_label_names(&[&dst])?;
                let srcs = args[3..]
                    .iter()
                    .map(|e| self.string_arg(e))
                    .collect::<ApiResult<Vec<_>>>()?;
                for s in &mut series {
                    let joined = srcs
                        .iter()
                        .map(|l| s.labels.get(l).cloned().unwrap_or_default())
                        .collect::<Vec<_>>()
                        .join(&sep);
                    if joined.is_empty() {
                        s.labels.remove(&dst);
                    } else {
                        s.labels.insert(dst.clone(), joined);
                    }
                }
                Ok(Value::Vector(series))
            }
            other => Err(unsupported(format!("function `{other}` is not supported"))),
        }
    }
}

