#!/bin/bash

# launches the dockerized kafka cluster

set -ev

iface=$(ifconfig | pcregrep -M -o '^[^\t:]+:([^\n]|\n\t)*status: active' | egrep -o -m 1 '^[^\t:]+')
export DOCKER_IP=$(ifconfig en0 | grep 'inet ' | awk '{print $2}')

echo The Docker IP is $DOCKER_IP

docker-compose up -d
