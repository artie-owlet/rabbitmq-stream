#!/bin/bash

set -e

cd "$(dirname ${BASH_SOURCE[0]})"

docker compose -f ./rabbitmq-cluster.yml down
