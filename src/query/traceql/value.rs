//! Values of TraceQL expressions and the comparison and arithmetic rules between them.

use regex::Regex;
use serde_json::Value;
use std::cell::RefCell;
use std::collections::HashMap;
use traceql_syntax::ast::{Kind, Op, Static, Status};

#[derive(Clone, Debug, PartialEq)]
pub enum Val {
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    /// Nanoseconds.
    Dur(i64),
    Status(Status),
    Kind(Kind),
}

pub fn from_json(value: &Value) -> Option<Val> {
    match value {
        Value::String(s) => Some(Val::Str(s.clone())),
        Value::Bool(b) => Some(Val::Bool(*b)),
        Value::Number(n) => n.as_i64().map(Val::Int).or_else(|| n.as_f64().map(Val::Float)),
        _ => None,
    }
}

pub fn from_static(value: &Static) -> Option<Val> {
    Some(match value {
        Static::Str(s) => Val::Str(s.clone()),
        Static::Int(v) => Val::Int(*v),
        Static::Float(v) => Val::Float(*v),
        Static::Bool(v) => Val::Bool(*v),
        Static::Duration(v) => Val::Dur(*v),
        Static::Status(v) => Val::Status(*v),
        Static::Kind(v) => Val::Kind(*v),
        Static::Nil => return None,
    })
}

pub fn status_of(code: i32) -> Status {
    match code {
        1 => Status::Ok,
        2 => Status::Error,
        _ => Status::Unset,
    }
}

pub fn kind_of(code: i32) -> Kind {
    match code {
        1 => Kind::Internal,
        2 => Kind::Server,
        3 => Kind::Client,
        4 => Kind::Producer,
        5 => Kind::Consumer,
        _ => Kind::Unspecified,
    }
}

impl Val {
    pub fn is_true(&self) -> bool {
        matches!(self, Val::Bool(true))
    }

    fn number(&self) -> Option<f64> {
        match self {
            Val::Int(v) | Val::Dur(v) => Some(*v as f64),
            Val::Float(v) => Some(*v),
            _ => None,
        }
    }
}

/// Compiled regular expressions of one query, so the same pattern is not built per span.
#[derive(Default)]
pub struct Regexes(RefCell<HashMap<String, Option<Regex>>>);

impl Regexes {
    fn is_match(&self, pattern: &str, value: &str) -> bool {
        let mut cache = self.0.borrow_mut();
        let re = cache
            .entry(pattern.to_string())
            .or_insert_with(|| Regex::new(&format!("^(?:{pattern})$")).ok());
        re.as_ref().is_some_and(|r| r.is_match(value))
    }
}

fn order(a: &Val, b: &Val) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Val::Int(x), Val::Int(y)) | (Val::Dur(x), Val::Dur(y)) => Some(x.cmp(y)),
        (Val::Str(x), Val::Str(y)) => Some(x.cmp(y)),
        (Val::Bool(x), Val::Bool(y)) => Some(x.cmp(y)),
        _ => a.number()?.partial_cmp(&b.number()?),
    }
}

/// `a op b` for the comparison operators. Values of unrelated types never compare.
pub fn compare(op: Op, a: &Val, b: &Val, regexes: &Regexes) -> bool {
    use std::cmp::Ordering::*;
    match op {
        Op::Re | Op::NotRe => match (a, b) {
            (Val::Str(value), Val::Str(pattern)) => regexes.is_match(pattern, value) == (op == Op::Re),
            _ => false,
        },
        Op::Eq | Op::Ne => {
            let same = match (a, b) {
                (Val::Status(x), Val::Status(y)) => Some(x == y),
                (Val::Kind(x), Val::Kind(y)) => Some(x == y),
                _ => order(a, b).map(|o| o == Equal),
            };
            same.is_some_and(|same| same == (op == Op::Eq))
        }
        Op::Gt | Op::Ge | Op::Lt | Op::Le => match order(a, b) {
            Some(o) => match op {
                Op::Gt => o == Greater,
                Op::Ge => o != Less,
                Op::Lt => o == Less,
                _ => o != Greater,
            },
            None => false,
        },
        _ => false,
    }
}

/// `a op b` for the arithmetic operators; `None` when the types do not combine or on division by
/// zero.
pub fn arith(op: Op, a: &Val, b: &Val) -> Option<Val> {
    match (a, b) {
        (Val::Int(x), Val::Int(y)) => int_op(op, *x, *y).map(Val::Int).or_else(|| float_op(op, *x as f64, *y as f64)),
        (Val::Dur(x), Val::Dur(y)) if matches!(op, Op::Add | Op::Sub | Op::Mod) => int_op(op, *x, *y).map(Val::Dur),
        (Val::Dur(x), Val::Int(y)) | (Val::Int(y), Val::Dur(x)) if matches!(op, Op::Mul) => Some(Val::Dur(x.checked_mul(*y)?)),
        _ => float_op(op, a.number()?, b.number()?),
    }
}

fn int_op(op: Op, x: i64, y: i64) -> Option<i64> {
    match op {
        Op::Add => x.checked_add(y),
        Op::Sub => x.checked_sub(y),
        Op::Mul => x.checked_mul(y),
        Op::Div if y != 0 => Some(x / y),
        Op::Mod if y != 0 => Some(x % y),
        _ => None,
    }
}

fn float_op(op: Op, x: f64, y: f64) -> Option<Val> {
    Some(Val::Float(match op {
        Op::Add => x + y,
        Op::Sub => x - y,
        Op::Mul => x * y,
        Op::Div if y != 0.0 => x / y,
        Op::Mod if y != 0.0 => x % y,
        Op::Pow => x.powf(y),
        _ => return None,
    }))
}

pub fn negate(value: &Val) -> Option<Val> {
    match value {
        Val::Int(v) => Some(Val::Int(-v)),
        Val::Float(v) => Some(Val::Float(-v)),
        Val::Dur(v) => Some(Val::Dur(-v)),
        _ => None,
    }
}
