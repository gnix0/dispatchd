#!/bin/sh

set -eu

IMAGE_NAME="task-orchestrator-proto-tools"
REPO_ROOT="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"

ensure_docker() {
  if ! command -v docker >/dev/null 2>&1; then
    echo "docker is required for protobuf tooling"
    exit 1
  fi
}

build_image() {
  docker build -t "${IMAGE_NAME}" "${REPO_ROOT}/tools/proto"
}

run_generate() {
  docker run --rm \
    -v "${REPO_ROOT}:/workspace" \
    -w /workspace/proto \
    "${IMAGE_NAME}" \
    sh -lc "buf generate"
}

run_check() {
  docker run --rm \
    -v "${REPO_ROOT}:/workspace" \
    -w /workspace/proto \
    "${IMAGE_NAME}" \
    sh -lc "buf lint"

  if git -C "${REPO_ROOT}" show-ref --verify --quiet refs/heads/main \
    && git -C "${REPO_ROOT}" cat-file -e main:proto/buf.yaml 2>/dev/null; then
    docker run --rm \
      -v "${REPO_ROOT}:/workspace" \
      -w /workspace \
      "${IMAGE_NAME}" \
      sh -lc "git config --global --add safe.directory /workspace && git config --global --add safe.directory /workspace/.git && buf breaking . --config proto/buf.yaml --path proto --against '.git#branch=main,subdir=proto'"
  else
    echo "proto baseline not present on main; skipping buf breaking check"
  fi
}

ensure_docker
build_image

case "${1:-}" in
  generate)
    run_generate
    ;;
  check)
    run_check
    ;;
  *)
    echo "usage: ./scripts/proto.sh [generate|check]"
    exit 1
    ;;
esac
