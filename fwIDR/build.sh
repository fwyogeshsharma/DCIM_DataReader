#!/bin/bash
# IDR Build Script
#
# Usage:
#   ./build.sh                   # all platforms
#   ./build.sh -p windows        # Windows only
#   ./build.sh -p linux          # Linux only
#   ./build.sh -p macos          # macOS (amd64 + arm64)

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

PLATFORM="all"
OUTPUT_DIR="build"

while [[ $# -gt 0 ]]; do
    case $1 in
        -p|--platform) PLATFORM="$2"; shift 2 ;;
        -o|--output)   OUTPUT_DIR="$2"; shift 2 ;;
        -h|--help)     echo "Usage: $0 [-p windows|linux|macos|all] [-o output-dir]"; exit 0 ;;
        *) echo -e "${RED}Unknown option: $1${NC}"; exit 1 ;;
    esac
done

echo -e "${CYAN}================================"
echo "IDR - Build Script"
echo -e "================================${NC}"
echo ""

if ! command -v go &> /dev/null; then
    echo -e "${RED}ERROR: Go not found. Install from https://go.dev/dl/${NC}"
    exit 1
fi

echo -e "${GREEN}[OK] $(go version)${NC}"
echo ""

VERSION="1.0.0"
BUILD_TIME=$(date '+%Y-%m-%d %H:%M:%S')
LDFLAGS="-s -w -X main.Version=$VERSION -X 'main.BuildTime=$BUILD_TIME'"

build_platform() {
    local os=$1 arch=$2 ext=$3
    local platform_dir="$OUTPUT_DIR/$os-$arch"
    local binary="idr$ext"
    local output_path="$platform_dir/$binary"

    echo -e "${YELLOW}Building for $os/$arch...${NC}"
    mkdir -p "$platform_dir"

    export GOOS=$os GOARCH=$arch CGO_ENABLED=0

    if go build -trimpath -ldflags "$LDFLAGS" -o "$output_path" .; then
        local size; size=$(du -h "$output_path" | cut -f1)
        echo -e "  ${GREEN}[OK] $binary ($size)${NC}"
        cp idr.yaml "$platform_dir/idr.yaml"
        echo -e "  ${GREEN}[OK] Copied idr.yaml${NC}"
    else
        echo -e "  ${RED}[FAILED] Build failed for $os/$arch${NC}"; exit 1
    fi
    echo ""
}

mkdir -p "$OUTPUT_DIR"

case "$PLATFORM" in
    windows) build_platform "windows" "amd64" ".exe" ;;
    linux)   build_platform "linux"   "amd64" ""     ;;
    macos)   build_platform "darwin" "amd64" ""; build_platform "darwin" "arm64" "" ;;
    all)
        build_platform "windows" "amd64" ".exe"
        build_platform "linux"   "amd64" ""
        build_platform "darwin"  "amd64" ""
        build_platform "darwin"  "arm64" ""
        ;;
    *) echo -e "${RED}Unknown platform: $PLATFORM${NC}"; exit 1 ;;
esac

echo -e "${GREEN}================================"
echo "Build complete! Output: $OUTPUT_DIR/"
echo -e "================================${NC}"
