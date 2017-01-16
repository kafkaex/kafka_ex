#!/bin/bash

# launches the dockerized kafka cluster

set -e

# Kafka needs to know our ip address so that it can advertise valid
# connnection details
if [ -z ${IP_IFACE} ]
then
  echo Detecting active network interface
	IP_IFACE=$(ifconfig | ./scripts/active_ifaces.sh | head -n 1 | cut -d ':' -f1)
fi

export DOCKER_IP=$(ifconfig ${IP_IFACE} | grep 'inet ' | awk '{print $2}' | cut -d ':' -f2)

# for debugging purposes
echo Detected active network interface ${IP_IFACE} with ip ${DOCKER_IP}

for i in 1 2 3
do
  port=$(expr 9092 + ${i} - 1)
  mkdir -p kafka${i}
  target=kafka${i}/server.properties.in
  cp ./server.properties ${target}
  sed -i.bak "s/@broker_id@/${i}/g" ${target}
  sed -i.bak "s/@port@/${port}/g" ${target}
  sed -i.bak "s|@pwd@||g" ${target}
  sed -i.bak "s/localhost:2181/zookeeper:2181/" ${target}
  echo "advertised.listeners=SSL://${DOCKER_IP}:${port}" >> ${target}
  echo "listeners=SSL://0.0.0.0:${port}" >> ${target}
done

docker-compose up -d
