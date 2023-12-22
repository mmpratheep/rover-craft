#!/bin/bash

# cargo install cargo-zigbuild // required for multi-platform build
# brew install zig
#  rustup target add x86_64-unknown-linux-gnu
cargo zigbuild --release --target x86_64-unknown-linux-gnu

cp ./target/x86_64-unknown-linux-gnu/release/rovercraft ./docker/node/

cd ./docker/test

docker-compose up -d --build
