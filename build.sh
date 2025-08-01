#!/bin/bash

set -e

echo "[ BUILD RELEASE ]"
BIN_DIR=$(pwd)/bin/
rm -rf "$BIN_DIR"
mkdir -p "$BIN_DIR/plugins"

cp shake.toml "$BIN_DIR"

VERSION=${VERSION:-"dev"}
if git rev-parse --is-inside-work-tree > /dev/null 2>&1; then
    COMMIT=${COMMIT:-$(git rev-parse --short HEAD)}
else
    COMMIT=${COMMIT:-"unknown"}
fi
LDFLAGS=${LDFLAGS:-"-X main.Version=${VERSION} -X main.GitCommit=${COMMIT}"}

dist() {
    echo "try build GOOS=$1 GOARCH=$2"
    export GOOS=$1
    export GOARCH=$2
    export CGO_ENABLED=0
    
    go build -v -trimpath -ldflags "${LDFLAGS}" -o "$BIN_DIR/redis-shake" "./cmd/redis-shake"

    # Build plugin in its own directory
    cd "./examples/gorm_entry_writer_demo"
    mkdir -p "../../$BIN_DIR/plugins"
    go build -buildmode=plugin -v -trimpath -ldflags "${LDFLAGS}" -o "$BIN_DIR/plugins/gorm_entry_writer_demo.so"
    cd "../.."

    unset GOOS
    unset GOARCH
    echo "build success GOOS=$1 GOARCH=$2"

    cd "$BIN_DIR"
    tar -czvf ./redis-shake-"$1"-"$2".tar.gz ./redis-shake ./shake.toml
    cd ..
}

if [ "$1" == "dist" ]; then
    echo "[ DIST ]"
    for g in "linux" "darwin" "windows"; do
        for a in "amd64" "arm64"; do
            dist "$g" "$a"
        done
    done
fi

# build the current platform
echo "try build for current platform"

go build -v -trimpath -ldflags "${LDFLAGS}" -o "$BIN_DIR/redis-shake" "./cmd/redis-shake"

# Build plugin in its own directory
cd "./examples/gorm_entry_writer_demo"
go build -buildmode=plugin -v -trimpath -ldflags "${LDFLAGS}" -o "$BIN_DIR/plugins/gorm_entry_writer_demo.so"
cd "../.."

echo "build success"
