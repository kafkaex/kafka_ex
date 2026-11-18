defmodule KafkaEx.Integration.ConsumerGroup.CoordinatorRediscoveryTest do
  @moduledoc """
  Integration regression for PR-3 / defect C-3.

  When a consumer-group request reaches a broker that is NOT the group coordinator,
  the broker returns NOT_COORDINATOR. The client must invalidate its cached
  coordinator and re-run FindCoordinator before retrying — not blind-retry the wrong
  broker (the rolling-deploy hang).

  We trigger a *real* NOT_COORDINATOR deterministically (no broker killing) by seeding
  the client's coordinator cache with the wrong broker via `:sys.replace_state`, then
  issuing a real consumer-group request against the live cluster.

  Pre-fix this returned `{:error, :not_coordinator}` (blind-retried the wrong broker);
  post-fix it re-discovers the real coordinator and succeeds.
  """
  use ExUnit.Case, async: false
  @moduletag :consumer_group

  import KafkaEx.TestHelpers
  import KafkaEx.IntegrationHelpers
  import KafkaEx.TestSupport.ProcessHelpers

  alias KafkaEx.API
  alias KafkaEx.Client
  alias KafkaEx.Client.MetadataStore.Snapshot

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, client} = Client.start_link(args, :no_name)
    on_exit(fn -> stop_safely(client) end)

    {:ok, %{client: client}}
  end

  @tag timeout: 60_000
  test "a consumer-group request to a stale coordinator re-discovers and succeeds", %{client: client} do
    topic = generate_random_string()
    group = generate_random_string()
    _ = create_topic(client, topic, partitions: 1)
    wait_for_topic_in_metadata(client, topic)

    partitions = [%{partition_num: 0}]

    # First request populates the real coordinator in the cache.
    assert {:ok, _} = API.fetch_committed_offset(client, group, topic, partitions)

    {:ok, metadata} = API.cluster_metadata(client)
    real_coord = metadata.consumer_group_coordinators[group]
    wrong_node = metadata.brokers |> Map.keys() |> Enum.find(&(&1 != real_coord))

    assert wrong_node,
           "need a multi-broker cluster to pick a non-coordinator broker (got brokers: " <>
             "#{inspect(Map.keys(metadata.brokers))}, coord: #{inspect(real_coord)})"

    # Seed the cache with the WRONG coordinator -> the next request hits a broker that
    # really is not the coordinator for this group and returns NOT_COORDINATOR.
    store = :sys.get_state(client).store.pid

    :sys.replace_state(store, fn {_, data} = s ->
      Snapshot.put_coordinator(data.table, group, wrong_node)
      s
    end)

    # PR-3: invalidate + re-discover + succeed.
    assert {:ok, _} = API.fetch_committed_offset(client, group, topic, partitions)

    # the cache self-corrected back to the real coordinator
    {:ok, healed_metadata} = API.cluster_metadata(client)
    healed = healed_metadata.consumer_group_coordinators[group]
    assert healed == real_coord
  end
end
