#!/bin/bash

# launches the dockerized kafka cluster

set -ev

iface=$(ifconfig | ./scripts/active_ifaces.sh | head -n 1 | cut -d ':' -f1)
export DOCKER_IP=$(ifconfig en0 | grep 'inet ' | awk '{print $2}')

echo The Docker IP is $DOCKER_IP

docker-compose up -d
