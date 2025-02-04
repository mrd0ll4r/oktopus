#!/bin/bash -e

mkdir -p out

# Build binaries.
# This builds twice: On a recent base image, and on an old one.
docker build -t ipfs-indexer-builder -f Dockerfile.builder .
docker build -t ipfs-indexer-old-builder -f Dockerfile.oldbuilder .

# Extract binaries to host.
# We use binaries from the oldbuilder environment, because our host runs a very
# old version of Ubuntu.
docker create --name extract ipfs-indexer-old-builder
docker cp extract:/ipfs-indexer/target/release/cid-worker ./out/
docker cp extract:/ipfs-indexer/target/release/post-cids ./out/
docker cp extract:/ipfs-indexer/target/release/block-worker ./out/
docker cp extract:/ipfs-indexer/target/release/file-worker ./out/
docker cp extract:/ipfs-indexer/target/release/directory-worker ./out/
docker cp extract:/ipfs-indexer/target/release/hamtshard-worker ./out/
docker cp extract:/ipfs-indexer/target/release/convert-cids-to-base16 ./out/
docker cp extract:/ipfs-indexer/target/release/notifier ./out/

docker rm extract

# Build worker images. These are based off the recent base image.
docker build -t ipfs-indexer-cid-worker -f Dockerfile.cid-worker .
docker build -t ipfs-indexer-block-worker -f Dockerfile.block-worker .
docker build -t ipfs-indexer-directory-worker -f Dockerfile.directory-worker .
docker build -t ipfs-indexer-hamtshard-worker -f Dockerfile.hamtshard-worker .
docker build -t ipfs-indexer-file-worker -f Dockerfile.file-worker .
