cp server.properties server.properties1
sed -i.bak 's/@broker_id@/1/g' server.properties1
sed -i.bak 's/@port@/9092/g' server.properties1
cp server.properties server.properties2
sed -i.bak 's/@broker_id@/2/g' server.properties2
sed -i.bak 's/@port@/9093/g' server.properties2
cp server.properties server.properties3
sed -i.bak 's/@broker_id@/3/g' server.properties3
sed -i.bak 's/@port@/9094/g' server.properties3
mkdir kafka
rm -rf /tmp/zookeeper
rm -rf /tmp/kafka-logs*
curl "http://apache.arvixe.com/kafka/0.9.0.0/kafka_2.10-0.9.0.0.tgz" | tar -zxv -C kafka --strip-components=1
cd kafka
bin/zookeeper-server-start.sh config/zookeeper.properties > /dev/null &
echo $! > zookeeper.pid
sleep 2
bin/kafka-server-start.sh ../server.properties1 > /dev/null &
echo $! > kafka1.pid
sleep 2
bin/kafka-server-start.sh ../server.properties2 > /dev/null &
echo $! > kafka2.pid
sleep 2
bin/kafka-server-start.sh ../server.properties3 > /dev/null &
echo $! > kafka3.pid
sleep 5
