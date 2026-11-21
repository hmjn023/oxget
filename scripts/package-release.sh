#!/usr/bin/env bash
set -euo pipefail

target="${1:?usage: package-release.sh TARGET}"
case "$target" in
  x86_64-unknown-linux-musl|aarch64-unknown-linux-musl) ;;
  *) echo "Unsupported release target: $target" >&2; exit 1 ;;
esac

version="$(python3 -c 'import tomllib; print(tomllib.load(open("Cargo.toml", "rb"))["package"]["version"])')"
if [[ "${GITHUB_REF_TYPE:-}" == tag && "${GITHUB_REF_NAME:-}" != "v$version" ]]; then
  echo "Release tag must match Cargo.toml version v$version" >&2
  exit 1
fi

binary="target/$target/release/oxget"
python3 - "$binary" <<'PY'
import subprocess
import sys

binary = sys.argv[1]
headers = subprocess.check_output(["readelf", "--program-headers", "--wide", binary], text=True)
dynamic = subprocess.check_output(["readelf", "--dynamic", "--wide", binary], text=True)
if "INTERP" in headers or "(NEEDED)" in dynamic:
    raise SystemExit("Release binary still depends on a dynamic loader or shared libraries")
print("Verified: no dynamic loader or shared-library dependencies")
PY
"$binary" --version

name="oxget-$version-$target"
stage="$(mktemp -d)"
trap 'rm -rf "$stage"' EXIT
mkdir -p "$stage/$name" dist
install -m 0755 "$binary" "$stage/$name/oxget"
install -m 0644 README.md LICENSE "$stage/$name/"
tar --create --gzip --file "dist/$name.tar.gz" --directory "$stage" "$name"
(
  cd dist
  sha256sum "$name.tar.gz" > "$name.tar.gz.sha256"
)
tar --list --gzip --file "dist/$name.tar.gz"
