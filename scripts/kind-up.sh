#!/bin/sh

set -eu

CLUSTER_NAME="${KIND_CLUSTER_NAME:-dispatchd}"
REPO_ROOT="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"

ensure_kind_cluster() {
  if ! kind get clusters | grep -qx "${CLUSTER_NAME}"; then
    kind create cluster --name "${CLUSTER_NAME}" --config "${REPO_ROOT}/deploy/kind/cluster.yaml"
  fi
}

build_images() {
  docker build --build-arg SERVICE=control-plane -t ghcr.io/gnix0/dispatchd-control-plane:dev "${REPO_ROOT}"
  docker build --build-arg SERVICE=scheduler -t ghcr.io/gnix0/dispatchd-scheduler:dev "${REPO_ROOT}"
  docker build --build-arg SERVICE=worker-gateway -t ghcr.io/gnix0/dispatchd-worker-gateway:dev "${REPO_ROOT}"
}

load_images() {
  kind load docker-image --name "${CLUSTER_NAME}" ghcr.io/gnix0/dispatchd-control-plane:dev
  kind load docker-image --name "${CLUSTER_NAME}" ghcr.io/gnix0/dispatchd-scheduler:dev
  kind load docker-image --name "${CLUSTER_NAME}" ghcr.io/gnix0/dispatchd-worker-gateway:dev
}

apply_manifests() {
  kubectl apply -k "${REPO_ROOT}/deploy/overlays/dev"
}

ensure_kind_cluster
build_images
load_images
apply_manifests
