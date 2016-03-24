#!/bin/bash
source $(dirname $0)/kafka_base_dir.sh
if [ -d ${base}/bin ]
then
  echo "Detected Kafka install at ${base}"
else
  echo "Installing Kafka to ${base}"
  mkdir -p ${base}
  curl http://apache.mirrors.tds.net/kafka/0.9.0.1/kafka_2.11-0.9.0.1.tgz | tar -zxv -C ${base} --strip-components=1
fi
