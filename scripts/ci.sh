#!/usr/bin/env bash
# Checks that run inside the pinned golang container, so the host needs no
# shell, Go toolchain or race-detector prerequisites.
set -euo pipefail
cd /src

# The container may run as a different user than the checkout owner; allow the
# module cache to be used without demanding an exact go.mod match.
export GOFLAGS=-mod=mod

case "${1:?usage: ci.sh fmt|vet|unit|integration|bdd|vuln|all}" in
fmt)
  bad=$(gofmt -l cmd internal tests scripts)
  if [ -n "$bad" ]; then
    echo "not gofmt-clean:"
    echo "$bad"
    exit 1
  fi
  echo "fmt: ok"
  ;;
vet)
  go vet -tags 'integration bdd' ./...
  ;;
unit)
  go test -race -count=1 ./...
  ;;
integration)
  : "${TEST_DATABASE_URL:?TEST_DATABASE_URL is injected by the test service}"
  go test -race -count=1 -tags=integration ./internal/...
  ;;
bdd)
  : "${BASE_URL:?BASE_URL is injected by the test service}"
  go test -count=1 -tags=bdd -timeout 30m ./tests/bdd/...
  ;;
vuln)
  go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...
  ;;
all)
  "$0" fmt
  "$0" vet
  "$0" unit
  ;;
*)
  echo "unknown check: $1" >&2
  exit 2
  ;;
esac
