#!/bin/sh

set -eu

printf 'restarting scheduler to exercise leader reacquisition...\n'
docker compose restart scheduler
docker compose ps scheduler

printf 'recent scheduler logs:\n'
docker compose logs --tail=30 scheduler
