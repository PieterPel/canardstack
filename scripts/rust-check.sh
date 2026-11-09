#!/usr/bin/env bash
# The Rust checks CI runs: `scripts/rust-check.sh fmt` or `scripts/rust-check.sh clippy`.
#
# Uses the cargo on PATH when it has rustfmt, clippy and a compiler. Otherwise, when Nix is
# available, runs the same check with a stable toolchain from nixpkgs, so the pre-commit hook works
# on a machine whose default cargo is a bare one.
set -euo pipefail
cd "$(dirname "$0")/.."

check=${1:-}
case "$check" in
  fmt) tool=fmt ;;
  clippy) tool=clippy ;;
  *) echo "usage: $0 fmt|clippy" >&2; exit 2 ;;
esac

if cargo "$tool" --version >/dev/null 2>&1 && rustc --version >/dev/null 2>&1; then
  case "$check" in
    fmt) exec cargo fmt --all -- --check ;;
    clippy) exec cargo clippy --workspace --all-targets --all-features --locked -- -D warnings ;;
  esac
fi

if [ -z "${RUST_CHECK_IN_NIX:-}" ] && command -v nix >/dev/null 2>&1; then
  RUST_CHECK_IN_NIX=1 exec nix shell nixpkgs#cargo nixpkgs#rustc nixpkgs#rustfmt nixpkgs#clippy \
    --command "$0" "$check"
fi

echo "cargo $tool and rustc are needed: install a Rust toolchain with the rustfmt and clippy components" >&2
echo "(rustup component add rustfmt clippy), or install Nix so this script can fetch one." >&2
exit 1
