#!/usr/bin/env bash

git fetch origin
git rebase origin/master
CONTAINER_ID="$(docker ps -a -q)"
echo "${CONTAINER_ID}"
$(docker stop $CONTAINER_ID)
docker-compose -f docker-compose-LocalExecutor.yml up --build -d

