#!/bin/sh
# install.sh — Auto-install script for dbsafe
# Usage: curl -sSfL https://raw.githubusercontent.com/nethalo/dbsafe/main/install.sh | sh -s -- -b /usr/local/bin
# Based on the godownloader pattern (https://github.com/goreleaser/godownloader)
set -e

OWNER=nethalo
REPO=dbsafe
BINARY=dbsafe
FORMAT=tar.gz
GITHUB=https://github.com

# Default install directory
BINDIR=${BINDIR:-./bin}

# ─── Logging ──────────────────────────────────────────────────────────────────

log_prefix() {
    echo "dbsafe-install"
}

log_info() {
    echo "$(log_prefix): $*"
}

log_err() {
    echo "$(log_prefix): $*" >&2
}

log_debug() {
    if [ "${DEBUG}" = "true" ]; then
        echo "$(log_prefix) [debug]: $*" >&2
    fi
}

log_crit() {
    log_err "$*"
    exit 1
}

# ─── OS / Arch Detection ──────────────────────────────────────────────────────

uname_os() {
    os=$(uname -s | tr '[:upper:]' '[:lower:]')
    case "$os" in
        darwin) echo "darwin" ;;
        linux)  echo "linux" ;;
        *)      echo "$os" ;;
    esac
}

uname_arch() {
    arch=$(uname -m)
    case "$arch" in
        x86_64 | amd64) echo "amd64" ;;
        aarch64 | arm64) echo "arm64" ;;
        *)              echo "$arch" ;;
    esac
}

uname_os_check() {
    os=$(uname_os)
    case "$os" in
        darwin | linux) return 0 ;;
        *)
            log_crit "Unsupported OS: $os. Only darwin and linux are supported."
            ;;
    esac
}

uname_arch_check() {
    arch=$(uname_arch)
    case "$arch" in
        amd64 | arm64) return 0 ;;
        *)
            log_crit "Unsupported architecture: $arch. Only amd64 and arm64 are supported."
            ;;
    esac
}

# ─── HTTP helpers ─────────────────────────────────────────────────────────────

http_download_curl() {
    local_file=$1
    source_url=$2
    header=$3
    if [ -z "$header" ]; then
        code=$(curl -w '%{http_code}' -sL -o "$local_file" "$source_url")
    else
        code=$(curl -w '%{http_code}' -sL -H "$header" -o "$local_file" "$source_url")
    fi
    if [ "$code" != "200" ]; then
        log_err "curl: HTTP $code for $source_url"
        return 1
    fi
    return 0
}

http_download_wget() {
    local_file=$1
    source_url=$2
    header=$3
    if [ -z "$header" ]; then
        wget -q -O "$local_file" "$source_url"
    else
        wget -q --header "$header" -O "$local_file" "$source_url"
    fi
}

http_download() {
    log_debug "http_download $2"
    if command -v curl >/dev/null 2>&1; then
        http_download_curl "$@"
        return
    elif command -v wget >/dev/null 2>&1; then
        http_download_wget "$@"
        return
    fi
    log_crit "Neither curl nor wget found. Please install one of them."
}

# Fetch URL content into stdout (for JSON parsing)
http_copy() {
    url=$1
    header=$2
    tmpfile=$(mktemp)
    http_download "$tmpfile" "$url" "$header"
    cat "$tmpfile"
    rm -f "$tmpfile"
}

# ─── GitHub Release Resolution ────────────────────────────────────────────────

# Resolve the latest release tag from github.com/releases/latest
# Uses the releases/latest page with Accept: application/json to avoid
# the 60 req/hr API rate limit on api.github.com
github_release() {
    owner=$1
    repo=$2
    # The /releases/latest page redirects to the tag URL; requesting JSON
    # returns {"tag_name":"v0.2.1",...} without hitting the rate-limited API.
    url="${GITHUB}/${owner}/${repo}/releases/latest"
    log_debug "Fetching latest release from $url"
    version=$(http_copy "$url" "Accept: application/json" | sed -n 's/.*"tag_name":"\([^"]*\)".*/\1/p')
    if [ -z "$version" ]; then
        log_crit "Could not determine latest release. Check your internet connection or specify a version explicitly."
    fi
    echo "$version"
}

# ─── Checksum Verification ────────────────────────────────────────────────────

hash_sha256() {
    target=$1
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$target" | cut -d ' ' -f1
    elif command -v gsha256sum >/dev/null 2>&1; then
        gsha256sum "$target" | cut -d ' ' -f1
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$target" | cut -d ' ' -f1
    elif command -v openssl >/dev/null 2>&1; then
        openssl dgst -sha256 "$target" | cut -d ' ' -f2
    else
        log_crit "No sha256 utility found. Install sha256sum, shasum, or openssl."
    fi
}

hash_sha256_verify() {
    target=$1
    checksums=$2
    basename=$3

    # Extract expected hash for this filename from checksums.txt
    expected=$(grep "  ${basename}$" "$checksums" | cut -d ' ' -f1)
    if [ -z "$expected" ]; then
        log_crit "Checksum for $basename not found in $checksums"
    fi

    actual=$(hash_sha256 "$target")
    if [ "$actual" != "$expected" ]; then
        log_crit "Checksum mismatch for $basename:
  expected: $expected
  actual:   $actual"
    fi
    log_debug "Checksum verified: $basename ($actual)"
}

# ─── Archive Extraction ───────────────────────────────────────────────────────

untar() {
    tarball=$1
    tar -xzf "$tarball"
}

# ─── Main ─────────────────────────────────────────────────────────────────────

execute() {
    # Parse arguments
    while getopts "b:dh" flag; do
        case "$flag" in
            b) BINDIR="$OPTARG" ;;
            d) DEBUG=true ;;
            h)
                echo "Usage: install.sh [-b install-dir] [-d] [version-tag]"
                echo ""
                echo "  -b DIR   Install binary to DIR (default: ./bin)"
                echo "  -d       Enable debug output"
                echo "  version  Pin a specific release tag (e.g. v0.2.1)"
                echo "           Defaults to the latest release."
                exit 0
                ;;
            *) ;;
        esac
    done
    shift $((OPTIND - 1))

    # Optional positional: specific version tag
    TAG=$1

    # Validate OS and arch early
    uname_os_check
    uname_arch_check
    OS=$(uname_os)
    ARCH=$(uname_arch)

    # Resolve version
    if [ -z "$TAG" ]; then
        log_info "Resolving latest release..."
        TAG=$(github_release "$OWNER" "$REPO")
    fi
    log_info "Installing ${BINARY} ${TAG}"

    # GoReleaser strips the 'v' prefix in archive filenames
    VERSION=${TAG#v}

    # Build artifact names matching GoReleaser name_template:
    # dbsafe_{{ .Version }}_{{ .Os }}_{{ .Arch }}.tar.gz
    ARCHIVE="${BINARY}_${VERSION}_${OS}_${ARCH}.${FORMAT}"
    CHECKSUMS="checksums.txt"
    BASE_URL="${GITHUB}/${OWNER}/${REPO}/releases/download/${TAG}"

    # Work in a temp directory
    TMPDIR=$(mktemp -d)
    trap 'rm -rf "$TMPDIR"' EXIT

    ARCHIVE_PATH="${TMPDIR}/${ARCHIVE}"
    CHECKSUMS_PATH="${TMPDIR}/${CHECKSUMS}"

    log_info "Downloading $ARCHIVE ..."
    http_download "$ARCHIVE_PATH" "${BASE_URL}/${ARCHIVE}"

    log_info "Downloading checksums..."
    http_download "$CHECKSUMS_PATH" "${BASE_URL}/${CHECKSUMS}"

    log_info "Verifying checksum..."
    hash_sha256_verify "$ARCHIVE_PATH" "$CHECKSUMS_PATH" "$ARCHIVE"

    # Extract into temp dir
    log_info "Extracting..."
    (cd "$TMPDIR" && untar "$ARCHIVE_PATH")

    # Install binary
    mkdir -p "$BINDIR"
    install -m 0755 "${TMPDIR}/${BINARY}" "${BINDIR}/${BINARY}"

    log_info "Installed ${BINDIR}/${BINARY}"
    log_info "Run '${BINDIR}/${BINARY} --version' to verify."
}

execute "$@"
