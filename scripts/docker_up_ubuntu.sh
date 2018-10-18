#!/bin/bash

# see docker_up.sh
# This script should be run from the project root

set -e

IP_IFACE="docker0"
DOCKER_IP=$(ip addr show dev $IP_IFACE | grep -o "inet[^/]*" | grep -o "[0-9]\{1,3\}[.][0-9.]*")

for i in 1 2 3
do
  port=$(expr 9092 + ${i} - 1)
  mkdir -p kafka${i}
  target=kafka${i}/server.properties.in
  cp ./server.properties ${target}
  # configure broker and port
  sed -i.bak "s/@broker_id@/${i}/g" ${target}
  sed -i.bak "s/@port@/${port}/g" ${target}
  # the @pwd become root directory so we get /ssl
  sed -i.bak "s|@pwd@||g" ${target}
  # point at zookeeper in docker
  sed -i.bak "s/localhost:2181/zookeeper:2181/" ${target}
  # delete the existing listeners line
  sed -i.bak "/^listeners=/d" ${target}
  # add an advertised.listeners and listeners together at the end
  echo "advertised.listeners=SSL://${DOCKER_IP}:${port}" >> ${target}
  echo "listeners=SSL://0.0.0.0:${port}" >> ${target}
done

docker-compose up -d

# create topics needed for testing
docker-compose exec kafka3 /bin/bash -c "KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 KAFKA_PORT=9094 KAFKA_CREATE_TOPICS=consumer_group_implementation_test:4:2,test0p8p0:4:2 create-topics.sh"
