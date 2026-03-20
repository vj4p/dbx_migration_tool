#!/usr/bin/env bash
# build_and_push.sh — Build Docker image and push to AWS GovCloud ECR
# Usage: ./deploy/scripts/build_and_push.sh [TAG]
# Requires: aws CLI v2, docker, jq

set -euo pipefail

# ── Config ────────────────────────────────────────────────────────────────────
AWS_REGION="${AWS_DEFAULT_REGION:-us-gov-west-1}"
AWS_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
ECR_REPO="dbx-migration-tool"
ECR_REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
IMAGE_TAG="${1:-$(git rev-parse --short HEAD 2>/dev/null || echo 'latest')}"
FULL_IMAGE="${ECR_REGISTRY}/${ECR_REPO}:${IMAGE_TAG}"
LATEST_IMAGE="${ECR_REGISTRY}/${ECR_REPO}:latest"

echo "==> AWS Account : ${AWS_ACCOUNT_ID}"
echo "==> Region      : ${AWS_REGION}"
echo "==> Image       : ${FULL_IMAGE}"

# ── Ensure ECR repo exists ────────────────────────────────────────────────────
echo "==> Ensuring ECR repository exists..."
aws ecr describe-repositories \
    --repository-names "${ECR_REPO}" \
    --region "${AWS_REGION}" > /dev/null 2>&1 \
|| aws ecr create-repository \
    --repository-name "${ECR_REPO}" \
    --region "${AWS_REGION}" \
    --image-scanning-configuration scanOnPush=true \
    --encryption-configuration encryptionType=AES256

# ── ECR login ─────────────────────────────────────────────────────────────────
echo "==> Authenticating with ECR..."
aws ecr get-login-password --region "${AWS_REGION}" \
  | docker login --username AWS --password-stdin "${ECR_REGISTRY}"

# ── Build ─────────────────────────────────────────────────────────────────────
echo "==> Building Docker image..."
docker build \
    --target runtime \
    --platform linux/amd64 \
    --build-arg BUILD_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --build-arg GIT_COMMIT="${IMAGE_TAG}" \
    --tag "${FULL_IMAGE}" \
    --tag "${LATEST_IMAGE}" \
    .

# ── Push ──────────────────────────────────────────────────────────────────────
echo "==> Pushing image to ECR..."
docker push "${FULL_IMAGE}"
docker push "${LATEST_IMAGE}"

echo ""
echo "✓ Successfully pushed: ${FULL_IMAGE}"
echo ""
echo "To deploy to ECS, update the task definition image URI to:"
echo "  ${FULL_IMAGE}"
echo ""
echo "Then run:"
echo "  aws ecs update-service \\"
echo "    --cluster <your-cluster> \\"
echo "    --service dbx-migration-tool \\"
echo "    --force-new-deployment \\"
echo "    --region ${AWS_REGION}"
