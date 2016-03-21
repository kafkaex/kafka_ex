if [ "$TRAVIS" = "true" ]
then
  base=~/kafka
else
  base=$(pwd)/_build/kafka/dist
fi
