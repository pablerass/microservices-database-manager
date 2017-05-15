#!/bin/bash -e

USER=pablerass
IMAGE=microservices-database-manager
VERSION=latest

docker login --username=$USER
docker build --tag $USER/$IMAGE:$VERSION .
docker push $USER/$IMAGE:$VERSION
