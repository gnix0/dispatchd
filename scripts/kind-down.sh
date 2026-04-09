#!/bin/sh

set -eu

CLUSTER_NAME="${KIND_CLUSTER_NAME:-task-orchestrator}"

kind delete cluster --name "${CLUSTER_NAME}"
