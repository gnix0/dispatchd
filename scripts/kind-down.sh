#!/bin/sh

set -eu

CLUSTER_NAME="${KIND_CLUSTER_NAME:-dispatchd}"

kind delete cluster --name "${CLUSTER_NAME}"
