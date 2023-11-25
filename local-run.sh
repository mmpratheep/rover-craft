#!/bin/bash

# cargo install cargo-zigbuild // required for multi-platform build
# brew install zig
cargo zigbuild --target x86_64-unknown-linux-gnu

cp ./target/x86_64-unknown-linux-gnu/debug/rovercraft ./docker/node/

cd ./docker/test

docker-compose up -d --build
