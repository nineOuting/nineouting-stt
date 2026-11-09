# DEV ECR 전송
기존 develop GHCR 멀티 플랫폼 빌드 및 모델 검사를 유지한다. merge Job의 metadata Digest를
전달받아 amd64만 DEV ECR로 복사한다. 다른 브랜치·태그·PR·수동 실행은 ECR Job을 실행하지 않는다.
DEV 계정 443845329525, Role nineouting-dev-stt-github, 저장소 nineouting-dev-stt가 선행 조건이다.
OIDC subject는 repo:nineOuting@341826954/nineouting-stt@1416587196:ref:refs/heads/develop 이다.
GitHub Environment는 사용하지 않는다. 장기 AWS 키는 불필요하다.
원본/목적지 config·layer Digest와 실제 linux/amd64를 검증한 후 image-result.txt를 Artifact로 제공한다.
태그는 SHA/run ID/attempt로 유일하게 생성한다. 실패 시 후속 배포는 없다.
이 Workflow는 인프라 Values 갱신, ArgoCD Sync, EKS 배포를 수행하지 않는다.
검증된 ECR Digest를 인프라 PR로 반영하고 별도 승인 후 수동 Sync한다.
