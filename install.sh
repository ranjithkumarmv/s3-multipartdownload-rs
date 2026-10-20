#!/bin/sh
# Install the s3-multipartdownload binary from the latest GitHub release.
#
#   curl -fsSL https://raw.githubusercontent.com/ranjithkumarmv/s3-multipartdownload-rs/main/install.sh | sh
#
# Environment:
#   INSTALL_DIR  where to put the binary (default: ~/.local/bin)
#   VERSION      a release tag such as v0.1.0 (default: latest)
#   BASE_URL     a mirror holding the release assets (overrides VERSION)
set -eu

REPO="ranjithkumarmv/s3-multipartdownload-rs"
BIN="s3-multipartdownload"
INSTALL_DIR="${INSTALL_DIR:-$HOME/.local/bin}"
VERSION="${VERSION:-latest}"

fail() {
    echo "install.sh: $*" >&2
    exit 1
}

os="$(uname -s)"
arch="$(uname -m)"
case "$os/$arch" in
    Linux/x86_64 | Linux/amd64) target="x86_64-unknown-linux-musl" ;;
    Linux/aarch64 | Linux/arm64) target="aarch64-unknown-linux-musl" ;;
    Darwin/arm64) target="aarch64-apple-darwin" ;;
    Darwin/x86_64) target="x86_64-apple-darwin" ;;
    *) fail "no prebuilt binary for $os/$arch. Install with: cargo install $BIN" ;;
esac

if [ -n "${BASE_URL:-}" ]; then
    base="$BASE_URL"
elif [ "$VERSION" = "latest" ]; then
    base="https://github.com/$REPO/releases/latest/download"
else
    base="https://github.com/$REPO/releases/download/$VERSION"
fi
asset="$BIN-$target.tar.gz"

command -v curl >/dev/null 2>&1 || fail "curl is required"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT INT TERM

echo "Downloading $asset ($VERSION)..."
curl -fsSL "$base/$asset" -o "$tmp/$asset" || fail "download failed: $base/$asset"
curl -fsSL "$base/$asset.sha256" -o "$tmp/$asset.sha256" || fail "checksum download failed"

expected="$(cut -d ' ' -f 1 < "$tmp/$asset.sha256")"
if command -v sha256sum >/dev/null 2>&1; then
    actual="$(sha256sum "$tmp/$asset" | cut -d ' ' -f 1)"
else
    actual="$(shasum -a 256 "$tmp/$asset" | cut -d ' ' -f 1)"
fi
[ "$expected" = "$actual" ] || fail "checksum mismatch for $asset"

tar -xzf "$tmp/$asset" -C "$tmp"
mkdir -p "$INSTALL_DIR"
install -m 755 "$tmp/$BIN-$target/$BIN" "$INSTALL_DIR/$BIN"
echo "Installed $INSTALL_DIR/$BIN"

case ":$PATH:" in
    *":$INSTALL_DIR:"*) ;;
    *) echo "Note: $INSTALL_DIR is not on your PATH. Add it, e.g.: export PATH=\"$INSTALL_DIR:\$PATH\"" ;;
esac
