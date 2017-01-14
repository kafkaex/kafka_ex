#!/bin/bash

# launches the dockerized kafka cluster

set -ev

# Kafka needs to know our ip address so that it can advertise valid
# connnection details
iface=$(ifconfig | ./scripts/active_ifaces.sh | head -n 1 | cut -d ':' -f1)
export DOCKER_IP=$(ifconfig ${iface} | grep 'inet ' | awk '{print $2}')

# for debugging purposes
echo Detected active network interface ${iface} with ip ${DOCKER_IP}

docker-compose up -d
