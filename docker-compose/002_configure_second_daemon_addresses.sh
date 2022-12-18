#!/bin/sh

set -ex

# Bump swarm addresses so the node correctly announces itself.
ipfs config --json 'Addresses.Swarm' '[
  "/ip4/0.0.0.0/tcp/4002",
  "/ip6/::/tcp/4002",
  "/ip4/0.0.0.0/udp/4002/quic",
  "/ip6/::/udp/4002/quic"
]'