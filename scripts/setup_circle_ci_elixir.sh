#!/bin/bash

set -ex

wget https://packages.erlang-solutions.com/erlang-solutions_1.0_all.deb
sudo dpkg -i erlang-solutions_1.0_all.deb
sudo apt-get update
sudo apt-get install esl-erlang=1:$OTP_VERSION
sudo apt-get install elixir=1:$ELIXIR_VERSION
