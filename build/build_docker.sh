#!/bin/bash

# Set variables for the image name and tag
IMAGE_NAME="my_version"
IMAGE_TAG="v1"

# Check if Dockerfile exists
if [ ! -f Dockerfile ]; then
  echo "Error: Dockerfile not found in the current directory."
  exit 1
fi

# Build the Docker image
echo "Building Docker image: $IMAGE_NAME:$IMAGE_TAG..."
docker build -t "$IMAGE_NAME:$IMAGE_TAG" .

if [ $? -ne 0 ]; then
  echo "Error: Failed to build the Docker image."
  exit 1
fi

echo "Docker image built successfully: $IMAGE_NAME:$IMAGE_TAG"

# Run a container from the built image
echo "Running a container from the image..."
docker run -it --rm "$IMAGE_NAME:$IMAGE_TAG"

if [ $? -ne 0 ]; then
  echo "Error: Failed to run the Docker container."
  exit 1
fi

echo "Container executed successfully."

