#!/bin/bash
# Finds a private IP for your machine. Tested on MacOS 10.14, Ubuntu 16.04, and Centos 7

# 192.168.x.x, 172.[16-31].x.x, 10.x.x.x
regex='inet (192\.168\.|172\.1[6789]\.|172\.2[0-9]\.|172\.3[01]\.|10\.)'

if [ "$(uname)" == "Darwin" ]; then
  # MacOS
  ifconfig | grep -E "$regex" | grep broadcast | awk -F ' ' '{print $2}' | head -n 1
else
  # Linux
  ip addr | grep -E "$regex" | grep brd | awk -F ' ' '{print $2}' | cut -d/ -f1 | head -n 1
fi

