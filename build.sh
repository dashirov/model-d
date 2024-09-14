#!/bin/bash

# TODO: Use Maven? (https://maven.apache.org/plugin-developers/cookbook/generate-assembly.html)

mkdir -p target

# Must match local.lambda_deployment_package in terraform/main.tf
export PROJECT_NAME="model-d"
# Build the image tagged as `$PROJECT_NAME` removing the previous one with this tag
docker build --rm --build-arg PROJECT_NAME=${PROJECT_NAME} --tag ${PROJECT_NAME} .
# Run the container mounting the `/var/tas/target/host` dir to the local `target` dir and removing the container after it exits
docker run --rm -v $PWD/target:/var/task/target/host ${PROJECT_NAME}
