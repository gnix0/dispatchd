#!/bin/sh

set -eu

TAG="${1:?usage: ./scripts/update-dev-image-tags.sh <tag>}"
FILE="$(CDPATH= cd -- "$(dirname "$0")/../deploy/overlays/dev" && pwd)/kustomization.yaml"

update_image_tag() {
  image_name="$1"
  perl -0pi -e "s|(newName: \Q${image_name}\E\n\s+newTag: )\S+|\${1}${TAG}|g" "${FILE}"
}

update_image_tag "ghcr.io/gnix0/dispatchd-control-plane"
update_image_tag "ghcr.io/gnix0/dispatchd-scheduler"
update_image_tag "ghcr.io/gnix0/dispatchd-worker-gateway"
