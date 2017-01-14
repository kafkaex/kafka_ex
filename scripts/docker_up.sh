#!/bin/bash

# launches the dockerized kafka cluster

set -ev

echo $(ifconfig)

iface=$(ifconfig | ./scripts/active_ifaces.sh | head -n 1 | cut -d ':' -f1)
echo Detected active network interface ${iface}

export DOCKER_IP=$(ifconfig ${iface} | grep 'inet ' | awk '{print $2}')

echo The Docker IP is $DOCKER_IP

docker-compose up -d
