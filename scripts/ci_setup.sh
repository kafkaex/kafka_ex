#!/bin/bash

# launch docker, etc.
#   specialized for running on travis

set -ev

# eth0 is the active network interface and the output of ifconfig is slightly
# different than on some other boxes
export DOCKER_IP=$(ifconfig eth0 | grep 'inet ' | awk '{print $2}' | cut -d':' -f2)

docker-compose up -d
