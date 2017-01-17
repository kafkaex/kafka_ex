#!/bin/bash

# This script is used when the docker kafka container starts
#
# This version is specialized from
# https://github.com/wurstmeister/kafka-docker/blob/master/start-kafka.sh
# to work with our setup (esp. with ssl)
#
# Note that we mainly manage the server.properties file during `docker_up.sh`;
# the wurstmeister/kafka behavior of translating KAFKA_* env vars to settings
# is removed from this script.

set -ev

cp ${KAFKA_HOME}/config/server.properties.in ${KAFKA_HOME}/config/server.properties

KAFKA_PID=0

# see https://medium.com/@gchudnov/trapping-signals-in-docker-containers-7a57fdda7d86#.bh35ir4u5
term_handler() {
  echo 'Stopping Kafka....'
  if [ $KAFKA_PID -ne 0 ]; then
    kill -s TERM "$KAFKA_PID"
    wait "$KAFKA_PID"
  fi
  echo 'Kafka stopped.'
  exit
}


# Capture kill requests to stop properly
trap "term_handler" SIGHUP SIGINT SIGTERM
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties &
KAFKA_PID=$!

wait "$KAFKA_PID"
