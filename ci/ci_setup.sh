#!/bin/bash

# launch docker, etc.
#   specialized for running on travis

set -ev

# eth0 is the active network interface and the output of ifconfig is slightly
# different than on some other boxes
export DOCKER_IP=$(ifconfig eth0 | grep 'inet ' | awk '{print $2}' | cut -d':' -f2)

echo Detected active network interface ${iface} with ip ${DOCKER_IP}

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
done

docker-compose up -d
