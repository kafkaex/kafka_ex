#!/bin/bash

# launches the dockerized kafka cluster

set -ev

if [ "$TRAVIS" = true ]
then
  iface=eth0
else
  iface=$(ifconfig | ./scripts/active_ifaces.sh | head -n 1 | cut -d ':' -f1)
fi

echo Detected active network interface ${iface}

export DOCKER_IP=$(ifconfig ${iface} | grep 'inet ' | awk '{print $2}')

echo Setting DOCKER_IP to $DOCKER_IP

docker-compose up -d
