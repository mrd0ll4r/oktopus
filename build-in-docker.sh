#!/bin/bash -e

mkdir -p out

# Build binaries
docker build -t ipfs-indexer-builder -f Dockerfile.builder .

# Extract binaries to host
docker create --name extract ipfs-indexer-builder
docker cp extract:/ipfs-indexer/target/release/cid-worker ./out/
docker cp extract:/ipfs-indexer/target/release/post-cids ./out/
docker cp extract:/ipfs-indexer/target/release/block-worker ./out/
docker cp extract:/ipfs-indexer/target/release/file-worker ./out/
docker cp extract:/ipfs-indexer/target/release/directory-worker ./out/
docker cp extract:/ipfs-indexer/target/release/hamtshard-worker ./out/

docker rm extract

# Build worker images
docker build -t ipfs-indexer-cid-worker -f Dockerfile.cid-worker .
docker build -t ipfs-indexer-block-worker -f Dockerfile.block-worker .
docker build -t ipfs-indexer-directory-worker -f Dockerfile.directory-worker .
docker build -t ipfs-indexer-hamtshard-worker -f Dockerfile.hamtshard-worker .
docker build -t ipfs-indexer-file-worker -f Dockerfile.file-worker .