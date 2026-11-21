defmodule KafkaEx.Integration.ConnectionStatePinTest do
  use ExUnit.Case, async: false
  @moduletag :lifecycle

  import KafkaEx.TestSupport.ProcessHelpers

  alias KafkaEx.API
  alias KafkaEx.Client
  alias KafkaEx.Cluster.Broker

  test "brokers returned by cluster_metadata/1 carry no socket" do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, client} = Client.start_link(args, :no_name)
    on_exit(fn -> stop_safely(client) end)

    {:ok, metadata} = API.cluster_metadata(client)
    brokers = Map.values(metadata.brokers)

    assert brokers != []
    refute Enum.any?(brokers, &Broker.connected?/1)
    assert Enum.all?(brokers, &(&1.socket == nil))
  end
end
