defmodule KafkaEx.Integration.ConsumerGroup.ListGroupsTest do
  use ExUnit.Case, async: false
  @moduletag :consumer_group

  import KafkaEx.TestHelpers
  import KafkaEx.IntegrationHelpers
  import KafkaEx.TestSupport.ProcessHelpers

  alias KafkaEx.Client
  alias KafkaEx.API
  alias KafkaEx.Messages.ConsumerGroupListing
  alias KafkaEx.Test.KayrockFixtures, as: Fixtures

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, pid} = Client.start_link(args, :no_name)
    on_exit(fn -> stop_safely(pid) end)

    {:ok, %{client: pid}}
  end

  test "list_groups returns a joined consumer group from across the cluster", %{client: client} do
    topic_name = generate_random_string()
    consumer_group = generate_random_string()
    _ = create_topic(client, topic_name)

    group_protocols = [
      %{name: "assign", metadata: Fixtures.group_protocol_metadata(topics: [topic_name])}
    ]

    opts = [session_timeout: 30_000, rebalance_timeout: 60_000, group_protocols: group_protocols]
    {:ok, _join} = join_group_with_retry(client, consumer_group, "", opts)

    assert {:ok, listings} = API.list_groups(client)

    assert Enum.any?(listings, fn
             %ConsumerGroupListing{group_id: ^consumer_group, protocol_type: "consumer"} -> true
             _ -> false
           end),
           "expected #{consumer_group} (protocol_type \"consumer\") in #{inspect(listings)}"
  end
end
