#!/bin/bash

# launches the dockerized kafka cluster

set -ev

if [ "$TRAVIS" = true ]
then
  iface=eth0
  docker_ip=$(ifconfig eth0 | grep 'inet ' | awk '{print $2}' | cut -d':' -f2)
else
  iface=$(ifconfig | ./scripts/active_ifaces.sh | head -n 1 | cut -d ':' -f1)
  docker_ip=$(ifconfig ${iface} | grep 'inet ' | awk '{print $2}')
fi

echo Detected active network interface ${iface}

export DOCKER_IP=${docker_ip}

echo Setting DOCKER_IP to $DOCKER_IP

docker-compose up -d
