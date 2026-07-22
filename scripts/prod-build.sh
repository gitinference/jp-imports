#!/usr/bin/env bash
set -euo pipefail

# --- 1. Configurations ---
IMAGE_NAME="ouslan/python-app"
IMAGE_TAG="${GITHUB_SHA:-latest}"
REGISTRY_IMAGE="docker.io/${IMAGE_NAME}"

echo "🚀 Starting Production Container Build Pipeline (via Docker/DinD)..."

# --- 2. Docker Hub Authentication ---
if [ -z "${DOCKER_PASSWORD:-}" ] || [ -z "${DOCKER_USERNAME:-}" ]; then
  echo "❌ Error: DOCKER_USERNAME or DOCKER_PASSWORD not set. Cannot push to registry."
  exit 1
else
  echo "🔐 Logging into Docker Hub..."
  # Changed podman to docker ⬇️
  echo "$DOCKER_PASSWORD" | docker login docker.io -u "$DOCKER_USERNAME" --password-stdin
fi

# --- 3. Build & Tag via Docker ---
echo "📦 Building image as '${REGISTRY_IMAGE}:${IMAGE_TAG}'..."
# Changed podman to docker ⬇️
docker build -f ./Dockerfile -t "${REGISTRY_IMAGE}:${IMAGE_TAG}" .

if [ "${IMAGE_TAG}" != "latest" ]; then
  echo "🏷️ Tagging image as 'latest'..."
  # Changed podman to docker ⬇️
  docker tag "${REGISTRY_IMAGE}:${IMAGE_TAG}" "${REGISTRY_IMAGE}:latest"
fi

# --- 4. Push via Docker ---
echo "📤 Pushing images to Docker Hub..."
# Changed podman to docker ⬇️
docker push "${REGISTRY_IMAGE}:${IMAGE_TAG}"

if [ "${IMAGE_TAG}" != "latest" ]; then
  # Changed podman to docker ⬇️
  docker push "${REGISTRY_IMAGE}:latest"
fi

echo "✅ Deployment completed successfully!"
