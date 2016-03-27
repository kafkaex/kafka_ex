This directory contains three scripts to:
* Install the Kafka version preferred for developing kafka_ex;
* Start a three-node Kafka (and Zookeeper cluster);
* Clean everything back up again.

The script `kafka_base_dir.sh` is used by the others to decide where to place Kafka; this way
we can drop Kafka in a directory cached betweeen builds by Travis, hopefully speeding up builds
and making them less dependent on the availability of Apache mirrors.
