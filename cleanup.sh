rm server.properties{1..3}{,.bak}
rm -rf /tmp/zookeeper
rm -rf /tmp/kafka-logs*
kill -9 `cat kafka/kafka*.pid`
kill -9 `cat kafka/zookeeper.pid`
rm -rf kafka
