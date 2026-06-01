defmodule KafkaEx.Integration.ConsumerGroup.ContractCharacterizationTest do
  use ExUnit.Case, async: false

  import KafkaEx.TestSupport.ConsumerGroupHelpers
  import KafkaEx.IntegrationHelpers, only: [create_topic: 3]
  import KafkaEx.TestHelpers, only: [generate_random_string: 0, uris: 0]

  alias KafkaEx.API
  alias KafkaEx.TestSupport.TestGenConsumer

  @moduletag :consumer_group
  @moduletag timeout: 60_000

  setup do
    {:ok, args} = KafkaEx.build_worker_options(uris: uris())
    {:ok, client} = API.start_client(args)
    on_exit(fn -> if Process.alive?(client), do: GenServer.stop(client) end)
    {:ok, client: client}
  end

  # Pins delivery count only: TestGenConsumer reports {:messages_received, count},
  # not message bodies, so per-offset ordering is not asserted here.
  test "delivers all produced messages", %{client: client} do
    topic = generate_random_string()
    group = generate_random_string()
    _ = create_topic(client, topic, partitions: 1)

    {:ok, cg} =
      start_test_consumer_group(
        uris: uris(),
        topics: [topic],
        group_prefix: group,
        consumer_module: TestGenConsumer,
        auto_offset_reset: :earliest,
        test_pid: self()
      )

    register_consumer_group_cleanup(cg)
    assert {:ok, :active} = wait_for_active(cg)
    assert {:ok, _} = wait_for_assignments(cg)

    values = for i <- 1..10, do: %{value: "msg-#{i}"}
    {:ok, _} = API.produce(client, topic, 0, values)

    # TestGenConsumer sends {:messages_received, count} per batch.
    assert wait_for_message_count(10, timeout: 30_000) >= 10
  end

  test "a clean stop commits progress so a new consumer resumes past it", %{client: client} do
    topic = generate_random_string()
    group = generate_random_string()
    _ = create_topic(client, topic, partitions: 1)

    {:ok, cg1} =
      start_test_consumer_group(
        uris: uris(), topics: [topic], group_prefix: group,
        consumer_module: TestGenConsumer, auto_offset_reset: :earliest, test_pid: self()
      )

    register_consumer_group_cleanup(cg1)
    assert {:ok, :active} = wait_for_active(cg1)
    assert {:ok, _} = wait_for_assignments(cg1)

    {:ok, _} = API.produce(client, topic, 0, for(i <- 1..5, do: %{value: "a-#{i}"}))
    assert wait_for_message_count(5, timeout: 30_000) >= 5

    :ok = stop_consumer_group(cg1)

    # Same group resumes; the committed offset means it does NOT re-deliver all 5.
    {:ok, _} = API.produce(client, topic, 0, for(i <- 1..3, do: %{value: "b-#{i}"}))

    {:ok, cg2} =
      start_test_consumer_group(
        uris: uris(), topics: [topic], group_prefix: group,
        consumer_module: TestGenConsumer, auto_offset_reset: :earliest, test_pid: self()
      )

    register_consumer_group_cleanup(cg2)
    assert {:ok, :active} = wait_for_active(cg2)

    # Characterization: the resumed consumer delivers at least the 3 new messages.
    # (At-least-once: it may re-deliver the last uncommitted batch — assert the lower bound.)
    assert wait_for_message_count(3, timeout: 30_000) >= 3
  end

  test "auto_offset_reset :earliest consumes from the start of an existing topic", %{client: client} do
    topic = generate_random_string()
    group = generate_random_string()
    _ = create_topic(client, topic, partitions: 1)

    # Produce BEFORE any consumer exists; :earliest must still see them.
    {:ok, _} = API.produce(client, topic, 0, for(i <- 1..4, do: %{value: "pre-#{i}"}))

    {:ok, cg} =
      start_test_consumer_group(
        uris: uris(), topics: [topic], group_prefix: group,
        consumer_module: TestGenConsumer, auto_offset_reset: :earliest, test_pid: self()
      )

    register_consumer_group_cleanup(cg)
    assert {:ok, :active} = wait_for_active(cg)
    assert wait_for_message_count(4, timeout: 30_000) >= 4
  end
end
