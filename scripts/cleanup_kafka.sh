#!/bin/bash
source $(dirname $0)/kafka_base_dir.sh

kill -9 `cat _build/kafka/*.pid`

rm -rf /tmp/zookeeper
rm -rf /tmp/kafka-logs*

cat <<EOF
Execute

rm -rf _build/kafka

to get rid of the Kafka distribution as well
EOF


