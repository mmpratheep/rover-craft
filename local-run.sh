#!/bin/bash

# cargo install cargo-zigbuild // required for multi-platform build
# brew install zig
cargo zigbuild --target aarch64-unknown-linux-gnu.2.17

cp ./target/aarch64-unknown-linux-gnu/debug/rovercraft ./docker/node/

cd ./docker/test

docker-compose up -d --build
