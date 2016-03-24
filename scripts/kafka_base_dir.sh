if [ "$TRAVIS" = "true" ]
then
  base=~/kafka
else
  PROJECT_ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. && pwd )"
  base=$(dirname $0)/../_build/kafka/dist
fi
