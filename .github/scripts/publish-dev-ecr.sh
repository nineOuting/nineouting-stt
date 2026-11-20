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
local_id=$(docker image inspect "$LOCAL_IMAGE" --format '{{.Id}}')
[[ "$local_id" =~ ^sha256:[0-9a-f]{64}$ ]] || { echo "::error::Invalid publish context or digest"; exit 1; }
test "$(docker image inspect "$LOCAL_IMAGE" --format '{{.Os}}/{{.Architecture}}')" = linux/amd64
docker tag "$LOCAL_IMAGE" "$target"
docker push "$target"
docker buildx imagetools inspect "$target" > ecr-manifest.txt
digest=$(awk '$1 == "Digest:" {print $2; exit}' ecr-manifest.txt)
[[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]] || { echo "::error::Invalid publish context or digest"; exit 1; }
docker buildx imagetools inspect --raw "$TARGET_IMAGE@$digest" > ecr-manifest-raw.json
test "$(jq -er '.config.digest' ecr-manifest-raw.json)" = "$local_id"
docker pull --platform linux/amd64 "$TARGET_IMAGE@$digest"
test "$(docker image inspect "$TARGET_IMAGE@$digest" --format '{{.Id}}')" = "$local_id"
test "$(docker image inspect "$TARGET_IMAGE@$digest" --format '{{.Os}}/{{.Architecture}}')" = linux/amd64
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
 echo '- Local/ECR config digest and pulled image ID: MATCH'
 echo '- Platform: linux/amd64'
} >> "${GITHUB_STEP_SUMMARY:?}"
