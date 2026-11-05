#!/usr/bin/env bash
# sync-ref.sh [--check]: puts DEPLOY_WORKFLOW_REF from deploy/versions.env into
# `uses:` of the reusable workflow in .github/workflows/release.yml.
# With --check it only compares and fails on a difference (a check of a PR).
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
ref=$(sed -n 's/^DEPLOY_WORKFLOW_REF=//p' "$root/deploy/versions.env")
[[ -n $ref ]] || { echo "deploy/versions.env has no DEPLOY_WORKFLOW_REF" >&2; exit 1; }
wf=$root/.github/workflows/release.yml
pattern='GreenOnGrey/nabu/\.github/workflows/deploy-component\.yml@'
current=$(grep -oE "${pattern}[^[:space:]]+" "$wf" | sed "s#.*@##" | sort -u)
if [[ ${1:-} == --check ]]; then
  if [[ $current != "$ref" ]]; then
    echo "::error file=.github/workflows/release.yml::uses points to ${current:-?}, while DEPLOY_WORKFLOW_REF=$ref: run deploy/sync-ref.sh" >&2
    exit 1
  fi
  echo "DEPLOY_WORKFLOW_REF=$ref matches release.yml"
  exit 0
fi
sed -i -E "s#(${pattern})[^[:space:]]+#\1${ref}#" "$wf"
echo "release.yml: deploy-component.yml@$ref"
