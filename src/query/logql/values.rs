//! Parsing of the values LogQL compares labels with: Go-style durations, byte sizes and IP
//! addresses, ranges and CIDRs.

use std::net::IpAddr;

/// `1h30m`, `250ms`, `1.5s`: Go's `time.ParseDuration`, in seconds.
pub fn parse_duration_secs(s: &str) -> Option<f64> {
    let (sign, mut rest) = match s.strip_prefix('-') {
        Some(r) => (-1.0, r),
        None => (1.0, s.strip_prefix('+').unwrap_or(s)),
    };
    if rest == "0" {
        return Some(0.0);
    }
    if rest.is_empty() {
        return None;
    }
    let mut total = 0.0;
    while !rest.is_empty() {
        let digits = rest.find(|c: char| !(c.is_ascii_digit() || c == '.')).unwrap_or(rest.len());
        let value: f64 = rest[..digits].parse().ok()?;
        rest = &rest[digits..];
        let unit_len = rest.find(|c: char| c.is_ascii_digit() || c == '.').unwrap_or(rest.len());
        let scale = match &rest[..unit_len] {
            "ns" => 1e-9,
            "us" | "µs" | "μs" => 1e-6,
            "ms" => 1e-3,
            "s" => 1.0,
            "m" => 60.0,
            "h" => 3600.0,
            _ => return None,
        };
        total += value * scale;
        rest = &rest[unit_len..];
    }
    Some(sign * total)
}

/// `10KB`, `1.5 GiB`, `512`: byte sizes as humanize parses them.
pub fn parse_bytes(s: &str) -> Option<f64> {
    let s = s.trim();
    let split = s.find(|c: char| !(c.is_ascii_digit() || c == '.')).unwrap_or(s.len());
    let value: f64 = s[..split].parse().ok()?;
    let scale = match s[split..].trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1.0,
        "kb" => 1e3,
        "mb" => 1e6,
        "gb" => 1e9,
        "tb" => 1e12,
        "pb" => 1e15,
        "eb" => 1e18,
        "kib" | "ki" | "k" => 1024.0,
        "mib" | "mi" => 1024f64.powi(2),
        "gib" | "gi" => 1024f64.powi(3),
        "tib" | "ti" => 1024f64.powi(4),
        "pib" | "pi" => 1024f64.powi(5),
        "eib" | "ei" => 1024f64.powi(6),
        _ => return None,
    };
    Some(value * scale)
}

fn as_u128(ip: IpAddr) -> (bool, u128) {
    match ip {
        IpAddr::V4(v4) => (false, u128::from(u32::from(v4))),
        IpAddr::V6(v6) => (true, u128::from(v6)),
    }
}

/// An address, a CIDR (`10.0.0.0/8`) or a range (`10.0.0.1-10.0.0.9`).
#[derive(Clone, Debug)]
pub struct IpMatcher {
    v6: bool,
    low: u128,
    high: u128,
}

impl IpMatcher {
    pub fn parse(spec: &str) -> Option<Self> {
        let spec = spec.trim();
        if let Some((a, b)) = spec.split_once('-') {
            let (v6a, low) = as_u128(a.trim().parse().ok()?);
            let (v6b, high) = as_u128(b.trim().parse().ok()?);
            return (v6a == v6b && low <= high).then_some(Self { v6: v6a, low, high });
        }
        if let Some((addr, bits)) = spec.split_once('/') {
            let (v6, base) = as_u128(addr.trim().parse().ok()?);
            let width: u32 = if v6 { 128 } else { 32 };
            let bits: u32 = bits.trim().parse().ok().filter(|b| *b <= width)?;
            let host = if bits == width { 0 } else { (1u128 << (width - bits)) - 1 };
            return Some(Self { v6, low: base & !host, high: base | host });
        }
        let (v6, value) = as_u128(spec.parse().ok()?);
        Some(Self { v6, low: value, high: value })
    }

    pub fn contains(&self, ip: IpAddr) -> bool {
        let (v6, value) = as_u128(ip);
        v6 == self.v6 && value >= self.low && value <= self.high
    }

    /// Whether the text holds an address inside the matcher.
    pub fn matches_text(&self, text: &str) -> bool {
        text.split(|c: char| !(c.is_ascii_hexdigit() || c == '.' || c == ':'))
            .map(|token| token.trim_matches(|c| c == '.' || c == ':'))
            .filter(|token| !token.is_empty())
            .filter_map(|token| token.parse::<IpAddr>().ok())
            .any(|ip| self.contains(ip))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_and_sizes_parse_like_go_and_humanize() {
        assert_eq!(parse_duration_secs("1h30m"), Some(5400.0));
        assert_eq!(parse_duration_secs("250ms"), Some(0.25));
        assert_eq!(parse_duration_secs("-1.5s"), Some(-1.5));
        assert_eq!(parse_duration_secs("5"), None);
        assert_eq!(parse_bytes("10KB"), Some(10_000.0));
        assert_eq!(parse_bytes("1.5 GiB"), Some(1.5 * 1024f64.powi(3)));
        assert_eq!(parse_bytes("abc"), None);
    }

    #[test]
    fn ip_matchers_cover_addresses_cidrs_and_ranges() {
        let cidr = IpMatcher::parse("192.168.1.0/24").unwrap();
        assert!(cidr.contains("192.168.1.77".parse().unwrap()));
        assert!(!cidr.contains("192.168.2.1".parse().unwrap()));
        let range = IpMatcher::parse("127.0.0.1-127.0.0.6").unwrap();
        assert!(range.contains("127.0.0.4".parse().unwrap()));
        assert!(!range.contains("127.0.0.7".parse().unwrap()));
        assert!(IpMatcher::parse("127.0.0.1").unwrap().matches_text(r#"{"ip":"127.0.0.1","x":1}"#));
        assert!(!IpMatcher::parse("::1").unwrap().matches_text("127.0.0.1"));
    }
}
