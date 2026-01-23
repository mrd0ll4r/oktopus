#!/bin/sh
set -ex

# This disables mDNS discovery, but maybe we want the nodes to know about each other?
ipfs config apply server

# Disable providing content.
# This should stop the nodes from announcing local content to the DHT, which
# should decrease load.
ipfs config Reprovider.Interval 0

# Limit memory a bit
ipfs config Swarm.ResourceMgr.MaxMemory 6442450944
