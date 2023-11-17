#!/bin/bash

set -e

cd "$(dirname ${BASH_SOURCE[0]})"

rm -f ./.erlang.cookie
echo echo $(($RANDOM * $RANDOM)) > ./.erlang.cookie
chmod 0400 ./.erlang.cookie

echo "Generating keys"
./gen-key.sh

docker compose -f ./rabbitmq-cluster.yml up --force-recreate -V --wait -d

echo "Waiting for rabbit@host1"
docker exec rabbitmq-stream-client-ci-1 rabbitmqctl -q wait /var/lib/rabbitmq/mnesia/rabbit@host1.pid -t 30
echo "Waiting for all nodes"
docker exec rabbitmq-stream-client-ci-1 rabbitmqctl -q await_online_nodes 3 -t 30

echo "READY"
