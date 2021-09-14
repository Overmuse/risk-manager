#!/usr/bin/env bash

set -x
set -eo pipefail

pushd scripts

docker-compose -f kafka.yml up -d --remove-orphans

popd
