#!/bin/bash

set -ex

curl -O https://raw.githubusercontent.com/kerl/kerl/master/kerl
chmod +x kerl
\curl -sSL https://raw.githubusercontent.com/taylor/kiex/master/install | bash -s

./kerl build $OTP_VERSION
./kerl install $OTP_VERSION

kiex install $ELIXIR_VERSION
