defmodule KafkaEx.Integration.Produce.MultiTopicTest do
  @moduledoc """
  Pin cross-topic metadata correctness on a single Client (issue #445).

  The reporter's symptom (response says Topic B when produced to Topic A)
  is unreachable by construction in v1.0 — the single GenServer serializes
  handle_call/3 and the broker echoes the request topic in the response.
  These tests lock the invariant in place against future refactors that
  might introduce parallelism.
  """
  use ExUnit.Case, async: true
  @moduletag :produce

  import KafkaEx.TestHelpers
  import KafkaEx.IntegrationHelpers

  alias KafkaEx.Client
  alias KafkaEx.API
  alias KafkaEx.Messages.RecordMetadata

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, pid} = Client.start_link(args, :no_name)

    {:ok, %{client: pid}}
  end

  describe "multi-topic produce on a single Client [issue #445]" do
    test "produce to two different topics returns correct topic per response", %{client: client} do
      topic_a = generate_random_string()
      topic_b = generate_random_string()

      _ = create_topic(client, topic_a)
      _ = create_topic(client, topic_b)

      a_results =
        for i <- 1..5 do
          {:ok, %RecordMetadata{} = meta} =
            API.produce(client, topic_a, 0, [%{value: "a#{i}"}])

          meta
        end

      b_results =
        for i <- 1..5 do
          {:ok, %RecordMetadata{} = meta} =
            API.produce(client, topic_b, 0, [%{value: "b#{i}"}])

          meta
        end

      assert Enum.all?(a_results, fn m -> m.topic == topic_a end),
             "expected every A response to echo #{topic_a}, got: #{inspect(Enum.map(a_results, & &1.topic))}"

      assert Enum.all?(b_results, fn m -> m.topic == topic_b end),
             "expected every B response to echo #{topic_b}, got: #{inspect(Enum.map(b_results, & &1.topic))}"

      refute Enum.any?(a_results, fn m -> m.topic == topic_b end)
      refute Enum.any?(b_results, fn m -> m.topic == topic_a end)
    end

    test "interleaved produce to two topics preserves topic identity per response", %{client: client} do
      topic_a = generate_random_string()
      topic_b = generate_random_string()

      _ = create_topic(client, topic_a)
      _ = create_topic(client, topic_b)

      interleaved =
        for i <- 1..10 do
          topic = if rem(i, 2) == 0, do: topic_b, else: topic_a

          {:ok, %RecordMetadata{} = meta} =
            API.produce(client, topic, 0, [%{value: "msg#{i}"}])

          {topic, meta.topic}
        end

      for {requested, returned} <- interleaved do
        assert requested == returned,
               "topic mismatch: requested #{requested}, got #{returned}"
      end
    end
  end
end
