defmodule KafkaEx.Integration.ConnectionStatePinTest do
  use ExUnit.Case, async: false
  @moduletag :lifecycle

  import KafkaEx.TestSupport.ProcessHelpers

  alias KafkaEx.API
  alias KafkaEx.Client
  alias KafkaEx.Cluster.Broker

  test "brokers returned by cluster_metadata/1 report connected? when connected" do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, client} = Client.start_link(args, :no_name)
    on_exit(fn -> stop_safely(client) end)

    {:ok, metadata} = API.cluster_metadata(client)
    brokers = Map.values(metadata.brokers)

    assert brokers != []
    assert Enum.all?(brokers, &Broker.connected?/1)
  end
end
