#!/bin/bash

set -ex

wget https://packages.erlang-solutions.com/erlang/debian/pool/esl-erlang_${OTP_VERSION}-1~ubuntu~xenial_amd64.deb
sudo apt install -y esl-erlang_${OTP_VERSION}-1~ubuntu~xenial_amd64.deb


wget https://packages.erlang-solutions.com/erlang/debian/pool/elixir_${ELIXIR_VERSION}-1~ubuntu~xenial_all.deb
sudo apt install -y elixir_${ELIXIR_VERSION}-1~ubuntu~xenial_all.deb
