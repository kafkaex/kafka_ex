#!/bin/bash
source $(dirname $0)/kafka_base_dir.sh
BUILD_DIR=${project_root}/_build/kafka

mkdir -p $BUILD_DIR

for i in 1 2 3
do
  port=$(expr 9092 + ${i} - 1)
  target=$BUILD_DIR/server.properties${i}
  cp ${project_root}/server.properties ${target}
  sed -i.bak "s/@broker_id@/${i}/g" ${target}
  sed -i.bak "s/@port@/${port}/g" ${target}
done

cd ${base}
bin/zookeeper-server-start.sh config/zookeeper.properties > /dev/null &
echo $! >${BUILD_DIR}/zookeeper.pid
sleep 2

for i in 1 2 3
do
  bin/kafka-server-start.sh $BUILD_DIR/server.properties${i} > /dev/null &
  echo $! >${BUILD_DIR}/kafka${i}.pid
  sleep 2
done

sleep 3
