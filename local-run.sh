#!/bin/bash

# cargo install cargo-zigbuild // required for multi-platform build
# brew install zig
#  rustup target add x86_64-unknown-linux-gnu
cargo zigbuild --target x86_64-unknown-linux-gnu --release

cp ./target/x86_64-unknown-linux-gnu/debug/rovercraft ./docker/node/

cd ./docker/test

docker-compose up -d --build
