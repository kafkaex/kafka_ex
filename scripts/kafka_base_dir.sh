if [ "$TRAVIS" = "true" ]
then
  base=~/kafka
else
  project_root="$( cd "$( dirname $0 )"/.. && pwd )"
  base=${project_root}/_build/kafka/dist
fi
