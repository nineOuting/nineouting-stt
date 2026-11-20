# DEV ECR 직접 게시
STT 이미지를 linux/amd64로 빌드하고 모델·전사 폴더 쓰기·배포 연결 유지 검사를 통과시킵니다.
검증한 동일 이미지를 Actions artifact로 전달해 DEV ECR에 직접 게시합니다.
develop push만 게시하며 PR/수동 실행은 검증만 합니다. 릴리스 태그/PROD 게시는 하지 않습니다.

DEV 계정 443845329525, Role nineouting-dev-stt-github, 저장소 nineouting-dev-stt를 사용합니다.
OIDC 및 기존 권한을 유지하며 장기 AWS 키와 Packages 권한은 사용하지 않습니다.
태그는 sha-<commit>-<run>-<attempt>이고, ECR manifest의 config digest와 빌드 이미지 ID,
다시 받은 이미지의 ID·linux/amd64 플랫폼을 검증합니다.
image-result.txt의 기존 4개 항목과 artifact 이름, ECR job 이름을 유지해 infra 승격 검증과 연결합니다.

검증 성공 후 infra dev values의 digest 변경 PR을 생성합니다.
인프라의 기존 자동 병합 검증 후 Argo CD가 Git 변경을 반영합니다.
이 workflow는 직접 Argo CD Sync나 kubectl 배포를 실행하지 않습니다.
prune=false, selfHeal=false 및 실제 클러스터 설정은 변경하지 않습니다.
