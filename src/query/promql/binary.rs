//! Binary operators: scalar and vector arithmetic, comparisons, vector matching, set operators.
//! The pairing logic lives in `crate::query::vector`.

use super::*;
use crate::query::vector::{arith, Applied, Card, Matching};

fn matching(modifier: &Option<parser::BinModifier>) -> Matching {
    let Some(m) = modifier else {
        return Matching::one_to_one();
    };
    let on = m.matching.as_ref().map(|l| match l {
        LabelModifier::Include(labels) => {
            (true, labels.labels.iter().map(|n| unescape(n)).collect())
        }
        LabelModifier::Exclude(labels) => {
            (false, labels.labels.iter().map(|n| unescape(n)).collect())
        }
    });
    let (card, include) = match &m.card {
        VectorMatchCardinality::ManyToOne(l) => (
            Card::ManyToOne,
            l.labels.iter().map(|n| unescape(n)).collect(),
        ),
        VectorMatchCardinality::OneToMany(l) => (
            Card::OneToMany,
            l.labels.iter().map(|n| unescape(n)).collect(),
        ),
        _ => (Card::OneToOne, Vec::new()),
    };
    Matching { on, card, include }
}

impl Ev<'_> {
    pub(super) fn binary(&self, b: &parser::BinaryExpr) -> ApiResult<Value> {
        let op = b.op.to_string();
        let comparison = b.op.is_comparison_operator();
        let as_bool = b.modifier.as_ref().is_some_and(|m| m.return_bool);
        let lhs = self.eval(&b.lhs)?;
        let rhs = self.eval(&b.rhs)?;
        let n = self.n();
        if matches!(lhs, Value::Samples(_)) || matches!(rhs, Value::Samples(_)) {
            return self.binary_samples(b, lhs, rhs);
        }
        match (lhs, rhs) {
            (Value::Scalar(a), Value::Scalar(c)) => {
                if comparison && !as_bool {
                    return Err(bad(
                        "comparisons between scalars must use the bool modifier",
                    ));
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
            (Value::Vector(series), Value::Scalar(c)) => Ok(Value::Vector(vector::vector_scalar(
                series, &c, &op, comparison, as_bool, false,
            ))),
            (Value::Scalar(c), Value::Vector(series)) => Ok(Value::Vector(vector::vector_scalar(
                series, &c, &op, comparison, as_bool, true,
            ))),
            (Value::Vector(l), Value::Vector(r)) => {
                let m = matching(&b.modifier);
                if b.op.is_set_operator() {
                    Ok(Value::Vector(vector::set_op(n, &op, l, r, &m.on)))
                } else {
                    vector::vector_vector(n, &op, comparison, as_bool, l, r, &m)
                        .map(Value::Vector)
                        .map_err(exec_err)
                }
            }
            _ => Err(bad("binary operator on a string")),
        }
    }
}
