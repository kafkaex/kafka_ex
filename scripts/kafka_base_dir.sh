if [ "$TRAVIS" = "true" ]
then
  base=~/kafka
else
  project_root="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
  base=${project_root}/_build/kafka/dist
fi
