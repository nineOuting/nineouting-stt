#!/usr/bin/env bash
# 버전 릴리스 (2026-11-27)
#   main: 머지마다 정식 버전. 처음 v1.0.0, 그다음 패치 번호만 하나씩 (v1.0.1, v1.0.2 ...)
#   dev:  DEV 이미지마다 <다음 정식 버전>-dev.<순번> 사전 릴리스 (v1.0.1-dev.1, v1.0.1-dev.2 ...)
#         DEV ECR 이미지에도 같은 버전 태그를 더한다 (배포는 지금처럼 digest로)
#
#   bash release.sh dev|main          실제 릴리스
#   DRY_RUN=1 bash release.sh dev     버전만 계산해 출력 (git 태그만 읽음)
set -euo pipefail
mode=${1:?usage: release.sh dev|main}
[[ "$mode" == dev || "$mode" == main ]] || { echo "::error::mode must be dev or main" >&2; exit 1; }
sha=${GITHUB_SHA:-$(git rev-parse HEAD)}

if [[ -z "${DRY_RUN:-}" ]]; then
  git fetch --quiet --force --tags origin
fi

# 가장 높은 정식 버전 (vX.Y.Z). 없으면 다음 버전은 1.0.0
# 1.0.0보다 낮은 예전 태그(v0.1.0 등)는 세지 않는다. 정식 버전은 v1.0.0부터 시작한다
latest=$(git tag -l 'v*' | grep -E '^v[1-9][0-9]*\.[0-9]+\.[0-9]+$' | sed 's/^v//' \
  | sort -t. -k1,1n -k2,2n -k3,3n | tail -n 1 || true)
if [[ -z "$latest" ]]; then
  next=1.0.0
else
  IFS=. read -r major minor patch <<< "$latest"
  next="$major.$minor.$((patch + 1))"
fi

# 다시 실행해도(재시도) 같은 커밋에 버전을 또 만들지 않는다
if [[ "$mode" == main ]]; then
  pattern='^v[0-9]+\.[0-9]+\.[0-9]+$'
else
  pattern='^v[0-9]+\.[0-9]+\.[0-9]+-dev\.[0-9]+$'
fi
existing=$(git tag --points-at "$sha" | grep -E "$pattern" | head -n 1 || true)

if [[ -n "$existing" ]]; then
  version=$existing
elif [[ "$mode" == main ]]; then
  version="v$next"
else
  last=$(git tag -l "v$next-dev.*" | grep -E "^v${next//./\\.}-dev\.[0-9]+$" | sed 's/.*-dev\.//' | sort -n | tail -n 1 || true)
  version="v$next-dev.$(( ${last:-0} + 1 ))"
fi

if [[ -n "${DRY_RUN:-}" ]]; then
  echo "$version"
  exit 0
fi

image_note=''
if [[ "$mode" == dev ]]; then
  result=verified-image/image-result.txt
  repository=$(awk -F= '$1 == "image.repository" {print $2}' "$result")
  digest=$(awk -F= '$1 == "image.digest" {print $2}' "$result")
  sha_tag=$(awk -F= '$1 == "image.tag" {print $2}' "$result")
  [[ "$digest" =~ ^sha256:[0-9a-f]{64}$ && "$repository" == "${TARGET_IMAGE:?}" ]] \
    || { echo "::error::Unexpected DEV image evidence" >&2; exit 1; }
  image_note=$(printf '## DEV 이미지\n- `%s@%s`\n- 태그: `%s`, `%s`\n\n' "$repository" "$digest" "$version" "$sha_tag")
fi

if [[ -z "$existing" ]]; then
  if [[ "$mode" == main ]]; then
    gh release create "$version" --target "$sha" --title "$version" --latest \
      --generate-notes --notes "운영 서버 준비 전이라 이미지는 만들지 않는다."
  else
    gh release create "$version" --target "$sha" --title "$version" --prerelease --latest=false \
      --generate-notes --notes "$image_note"
  fi
  echo "Released $version at $sha"
else
  echo "Already released $version at $sha"
fi

# DEV ECR 이미지에 버전 태그를 더한다. 같은 태그가 이미 그 이미지에 있으면 넘어간다 (태그는 IMMUTABLE)
if [[ "$mode" == dev ]]; then
  name=${repository#*/}
  if aws ecr describe-images --repository-name "$name" --image-ids imageTag="$version" \
      --query 'imageDetails[0].imageDigest' --output text 2>/dev/null | grep -qx "$digest"; then
    echo "ECR already tagged $version"
  else
    manifest=$(aws ecr batch-get-image --repository-name "$name" --image-ids imageDigest="$digest" \
      --query 'images[0].imageManifest' --output text)
    media_type=$(aws ecr batch-get-image --repository-name "$name" --image-ids imageDigest="$digest" \
      --query 'images[0].imageManifestMediaType' --output text)
    aws ecr put-image --repository-name "$name" --image-tag "$version" \
      --image-manifest "$manifest" --image-manifest-media-type "$media_type" > /dev/null
    echo "ECR tagged $repository:$version -> $digest"
  fi
  {
    echo "### $version"
    echo "- 이미지: \`$repository@$digest\`"
  } >> "${GITHUB_STEP_SUMMARY:-/dev/null}"
fi
