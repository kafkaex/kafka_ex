#!/bin/bash

# launches the dockerized kafka cluster

set -ev

# Kafka needs to know our ip address so that it can advertise valid
# connnection details
iface=$(ifconfig | ./scripts/active_ifaces.sh | head -n 1 | cut -d ':' -f1)
export DOCKER_IP=$(ifconfig ${iface} | grep 'inet ' | awk '{print $2}')

# for debugging purposes
echo Detected active network interface ${iface} with ip ${DOCKER_IP}

for i in 1 2 3
do
  port=9092
  mkdir -p kafka${i}
  target=kafka${i}/server.properties.in
  cp ./server.properties ${target}
  sed -i.bak "s/@broker_id@/${i}/g" ${target}
  sed -i.bak "s/@port@/${port}/g" ${target}
  sed -i.bak "s|@pwd@||g" ${target}
  sed -i.bak "s/localhost:2181/zookeeper:2181/" ${target}
done

docker-compose up -d
