#!/usr/bin/env bash
set -euo pipefail
# Publish the exact locally built image; never deploy or modify GitOps.
test "${GITHUB_REPOSITORY:?}" = nineOuting/nineouting-stt
test "${GITHUB_REF:?}" = refs/heads/develop
test "${GITHUB_EVENT_NAME:?}" = push
test "${EXPECTED_ACCOUNT:?}" = 443845329525
test "${AWS_REGION:?}" = ap-northeast-2
test "${TARGET_IMAGE:?}" = 443845329525.dkr.ecr.ap-northeast-2.amazonaws.com/nineouting-dev-stt
[[ "${GITHUB_SHA:?}" =~ ^[0-9a-f]{40}$ ]] || { echo "::error::Invalid publish context or digest"; exit 1; }
[[ "${GITHUB_RUN_ID:?}" =~ ^[0-9]+$ && "${GITHUB_RUN_ATTEMPT:?}" =~ ^[0-9]+$ ]] || { echo "::error::Invalid publish context or digest"; exit 1; }
test "${LOCAL_IMAGE:?}" = nineouting-verified:local
tag="sha-${GITHUB_SHA}-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
target="$TARGET_IMAGE:$tag"
fail() { echo "::error::$*" >&2; exit 1; }
require_equal() {
 if [[ "$2" != "$3" ]]; then
  fail "$1 mismatch: expected=$2 actual=$3"
 fi
}
# .Id can be a manifest digest with Docker's containerd image store.
# Hash the saved config bytes instead, for both classic and containerd stores.
workdir=$(mktemp -d)
trap 'rm -rf "$workdir"' EXIT
config_digest() {
 local image="$1" label="$2"
 docker image save --output "$workdir/image.tar" "$image" ||
  fail "$label: could not save image for config verification"
 python3 - "$workdir/image.tar" <<'PY' || fail "$label: could not read saved image config"
import hashlib
import json
import sys
import tarfile

with tarfile.open(sys.argv[1]) as archive:
    manifest_file = archive.extractfile("manifest.json")
    if manifest_file is None:
        raise ValueError("Saved image has no manifest.json")
    manifests = json.load(manifest_file)
    if len(manifests) != 1:
        raise ValueError("Expected exactly one saved image manifest")
    config_file = archive.extractfile(manifests[0]["Config"])
    if config_file is None:
        raise ValueError("Saved image has no config file")
    config = config_file.read()
    json.loads(config)
    print("sha256:" + hashlib.sha256(config).hexdigest())
PY
}
local_config=$(config_digest "$LOCAL_IMAGE" "Local image")
require_equal "Local platform" linux/amd64 "$(docker image inspect "$LOCAL_IMAGE" --format '{{.Os}}/{{.Architecture}}')"
docker tag "$LOCAL_IMAGE" "$target"
docker push "$target" || fail "ECR push failed: $target"
docker buildx imagetools inspect "$target" > ecr-manifest.txt || fail "Could not inspect ECR image: $target"
digest=$(awk '$1 == "Digest:" {print $2; exit}' ecr-manifest.txt)
[[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]] || { echo "::error::Invalid publish context or digest"; exit 1; }
docker buildx imagetools inspect --raw "$TARGET_IMAGE@$digest" > ecr-manifest-raw.json || fail "Could not read ECR manifest: $digest"
remote_config=$(jq -er '.config.digest' ecr-manifest-raw.json) || fail "ECR manifest has no config digest"
[[ "$remote_config" =~ ^sha256:[0-9a-f]{64}$ ]] || fail "Invalid ECR config digest: $remote_config"
require_equal "Local/ECR config digest" "$local_config" "$remote_config"
docker pull --platform linux/amd64 "$TARGET_IMAGE@$digest" || fail "ECR pull failed: $digest"
pulled_config=$(config_digest "$TARGET_IMAGE@$digest" "Pulled image")
require_equal "Local/pulled config digest" "$local_config" "$pulled_config"
require_equal "Pulled platform" linux/amd64 "$(docker image inspect "$TARGET_IMAGE@$digest" --format '{{.Os}}/{{.Architecture}}')"
image_uri="$TARGET_IMAGE@$digest"
printf 'image.repository=%s\nimage.digest=%s\nimage.tag=%s\nimage.uri=%s\n' \
 "$TARGET_IMAGE" "$digest" "$tag" "$image_uri" > image-result.txt
{
 echo "image_repository=$TARGET_IMAGE"
 echo "image_digest=$digest"
 echo "image_uri=$image_uri"
 echo "image_tag=$tag"
} >> "${GITHUB_OUTPUT:?}"
{
 echo '## DEV ECR Publish PASS'
 echo "- ECR image: $image_uri"
 echo '- Local/ECR/pulled config digests: MATCH'
 echo '- Platform: linux/amd64'
} >> "${GITHUB_STEP_SUMMARY:?}"
