#!/bin/bash

set -ex

wget -O- https://packages.erlang-solutions.com/erlang/debian/pool/esl-erlang_${OTP_VERSION}-1~ubuntu~xenial_amd64.deb
sudo dpkg -i esl-erlang_${OTP_VERSION}-1~ubuntu~xenial_amd64.deb


wget -O- https://packages.erlang-solutions.com/erlang/debian/pool/elixir_${ELIXIR_VERSION}-1~ubuntu~xenial_all.deb
sudo dpkg -i elixir_${ELIXIR_VERSION}-1~ubuntu~xenial_all.deb
