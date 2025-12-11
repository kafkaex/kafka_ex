defmodule KafkaEx.Integration.ProduceKayrockTest do
  use ExUnit.Case, async: true
  import KafkaEx.TestHelpers
  import KafkaEx.IntegrationHelpers

  @moduletag :integration

  alias KafkaEx.New.Client
  alias KafkaEx.New.KafkaExAPI, as: API
  alias KafkaEx.New.Structs.Produce

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, pid} = Client.start_link(args, :no_name)

    {:ok, %{client: pid}}
  end

  describe "produce/4 basic functionality" do
    test "produces single message to topic", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      messages = [%{value: "hello world"}]
      {:ok, result} = API.produce(client, topic_name, 0, messages)

      assert %Produce{} = result
      assert result.topic == topic_name
      assert result.partition == 0
      assert result.base_offset >= 0
    end

    test "produces multiple messages to topic", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      messages = [
        %{value: "message 1"},
        %{value: "message 2"},
        %{value: "message 3"}
      ]

      {:ok, result} = API.produce(client, topic_name, 0, messages)

      assert %Produce{} = result
      assert result.base_offset >= 0
    end

    test "produces message with key", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      messages = [%{value: "hello", key: "greeting"}]
      {:ok, result} = API.produce(client, topic_name, 0, messages)

      assert %Produce{} = result
      assert result.base_offset >= 0
    end

    test "produces message with nil key", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      messages = [%{value: "hello", key: nil}]
      {:ok, result} = API.produce(client, topic_name, 0, messages)

      assert %Produce{} = result
      assert result.base_offset >= 0
    end

    test "produces to different partitions", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 3)

      # Produce to partition 0
      {:ok, result0} = API.produce(client, topic_name, 0, [%{value: "p0"}])
      assert result0.partition == 0

      # Produce to partition 1
      {:ok, result1} = API.produce(client, topic_name, 1, [%{value: "p1"}])
      assert result1.partition == 1

      # Produce to partition 2
      {:ok, result2} = API.produce(client, topic_name, 2, [%{value: "p2"}])
      assert result2.partition == 2
    end

    test "increments offset with each produce", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, result1} = API.produce(client, topic_name, 0, [%{value: "first"}])
      {:ok, result2} = API.produce(client, topic_name, 0, [%{value: "second"}])
      {:ok, result3} = API.produce(client, topic_name, 0, [%{value: "third"}])

      assert result2.base_offset == result1.base_offset + 1
      assert result3.base_offset == result2.base_offset + 1
    end

    test "produces empty value", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      messages = [%{value: ""}]
      {:ok, result} = API.produce(client, topic_name, 0, messages)

      assert %Produce{} = result
      assert result.base_offset >= 0
    end

    test "produces binary value with special characters", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      binary_value = <<0, 1, 2, 255, 254, 253>>
      messages = [%{value: binary_value}]
      {:ok, result} = API.produce(client, topic_name, 0, messages)

      assert %Produce{} = result
      assert result.base_offset >= 0
    end
  end

  describe "produce/5 with options" do
    test "produces with acks=1 (leader only)", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      messages = [%{value: "acks=1 test"}]
      {:ok, result} = API.produce(client, topic_name, 0, messages, acks: 1)

      assert %Produce{} = result
      assert result.base_offset >= 0
    end

    test "produces with acks=-1 (all ISR)", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      messages = [%{value: "acks=-1 test"}]
      {:ok, result} = API.produce(client, topic_name, 0, messages, acks: -1)

      assert %Produce{} = result
      assert result.base_offset >= 0
    end
  end

  describe "produce/5 with compression" do
    test "produces with gzip compression", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      messages = [
        %{value: "gzip message 1"},
        %{value: "gzip message 2"},
        %{value: "gzip message 3"}
      ]

      {:ok, result} = API.produce(client, topic_name, 0, messages, compression: :gzip)

      assert %Produce{} = result
      assert result.base_offset >= 0
    end

    test "produces with snappy compression", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      messages = [
        %{value: "snappy message 1"},
        %{value: "snappy message 2"},
        %{value: "snappy message 3"}
      ]

      {:ok, result} = API.produce(client, topic_name, 0, messages, compression: :snappy)

      assert %Produce{} = result
      assert result.base_offset >= 0
    end

    test "produces with no compression (explicit)", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      messages = [%{value: "no compression"}]
      {:ok, result} = API.produce(client, topic_name, 0, messages, compression: :none)

      assert %Produce{} = result
      assert result.base_offset >= 0
    end
  end

  describe "produce/5 with API versions" do
    test "produces with API V0", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      messages = [%{value: "v0 message"}]
      {:ok, result} = API.produce(client, topic_name, 0, messages, api_version: 0)

      assert %Produce{} = result
      assert result.base_offset >= 0
      # V0 doesn't have throttle_time_ms or log_append_time
      assert is_nil(result.throttle_time_ms)
      assert is_nil(result.log_append_time)
    end

    test "produces with API V1", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      messages = [%{value: "v1 message"}]
      {:ok, result} = API.produce(client, topic_name, 0, messages, api_version: 1)

      assert %Produce{} = result
      assert result.base_offset >= 0
      # V1 has throttle_time_ms
      assert is_integer(result.throttle_time_ms) or is_nil(result.throttle_time_ms)
    end

    test "produces with API V2 (default)", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      messages = [%{value: "v2 message"}]
      {:ok, result} = API.produce(client, topic_name, 0, messages, api_version: 2)

      assert %Produce{} = result
      assert result.base_offset >= 0
      # V2 has log_append_time
      # log_append_time may be -1 if broker uses CreateTime
    end

    test "produces with API V3 (RecordBatch format)", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      messages = [%{value: "v3 message"}]
      {:ok, result} = API.produce(client, topic_name, 0, messages, api_version: 3)

      assert %Produce{} = result
      assert result.base_offset >= 0
    end

    test "produces with API V5 (includes log_start_offset)", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      messages = [%{value: "v5 message"}]
      {:ok, result} = API.produce(client, topic_name, 0, messages, api_version: 5)

      assert %Produce{} = result
      assert result.base_offset >= 0
      # V5 includes log_start_offset
      assert is_integer(result.log_start_offset) or is_nil(result.log_start_offset)
    end
  end

  describe "produce/5 with headers (V3+)" do
    test "produces message with headers using V3", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      messages = [
        %{
          value: "message with headers",
          headers: [{"content-type", "application/json"}, {"trace-id", "abc123"}]
        }
      ]

      {:ok, result} = API.produce(client, topic_name, 0, messages, api_version: 3)

      assert %Produce{} = result
      assert result.base_offset >= 0
    end

    test "produces message with empty headers list", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      messages = [%{value: "message with empty headers", headers: []}]
      {:ok, result} = API.produce(client, topic_name, 0, messages, api_version: 3)

      assert %Produce{} = result
      assert result.base_offset >= 0
    end

    test "produces message with nil headers", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      messages = [%{value: "message with nil headers", headers: nil}]
      {:ok, result} = API.produce(client, topic_name, 0, messages, api_version: 3)

      assert %Produce{} = result
      assert result.base_offset >= 0
    end
  end

  describe "produce/5 with timestamp (V2+)" do
    test "produces message with custom timestamp", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      timestamp = System.system_time(:millisecond)
      messages = [%{value: "message with timestamp", timestamp: timestamp}]

      {:ok, result} = API.produce(client, topic_name, 0, messages, api_version: 2)

      assert %Produce{} = result
      assert result.base_offset >= 0
    end

    test "produces message with timestamp using V3", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      timestamp = System.system_time(:millisecond)
      messages = [%{value: "v3 with timestamp", timestamp: timestamp}]

      {:ok, result} = API.produce(client, topic_name, 0, messages, api_version: 3)

      assert %Produce{} = result
      assert result.base_offset >= 0
    end
  end

  describe "produce_one/4 convenience function" do
    test "produces single message", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, result} = API.produce_one(client, topic_name, 0, "simple message")

      assert %Produce{} = result
      assert result.topic == topic_name
      assert result.partition == 0
      assert result.base_offset >= 0
    end

    test "produces message with empty value", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, result} = API.produce_one(client, topic_name, 0, "")

      assert %Produce{} = result
      assert result.base_offset >= 0
    end
  end

  describe "produce_one/5 with options" do
    test "produces message with key", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, result} = API.produce_one(client, topic_name, 0, "hello", key: "greeting")

      assert %Produce{} = result
      assert result.base_offset >= 0
    end

    test "produces message with key and compression", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, result} =
        API.produce_one(client, topic_name, 0, "compressed message",
          key: "my-key",
          compression: :gzip
        )

      assert %Produce{} = result
      assert result.base_offset >= 0
    end

    test "produces message with headers using V3", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, result} =
        API.produce_one(client, topic_name, 0, "event data",
          key: "event-1",
          headers: [{"content-type", "application/json"}],
          api_version: 3
        )

      assert %Produce{} = result
      assert result.base_offset >= 0
    end

    test "produces message with timestamp", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      timestamp = System.system_time(:millisecond)

      {:ok, result} =
        API.produce_one(client, topic_name, 0, "timestamped",
          timestamp: timestamp,
          api_version: 3
        )

      assert %Produce{} = result
      assert result.base_offset >= 0
    end

    test "produces with all options combined", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      timestamp = System.system_time(:millisecond)

      {:ok, result} =
        API.produce_one(client, topic_name, 0, "fully loaded message",
          key: "full-key",
          timestamp: timestamp,
          headers: [{"x-custom", "value"}],
          acks: -1,
          timeout: 10_000,
          compression: :gzip,
          api_version: 3
        )

      assert %Produce{} = result
      assert result.base_offset >= 0
    end
  end

  describe "produce error handling" do
    test "returns error for non-existent topic with auto-create disabled", %{client: client} do
      # This depends on broker configuration (auto.create.topics.enable)
      # If auto-create is enabled, this will succeed
      # If disabled, it should return an error
      topic_name = "nonexistent_#{generate_random_string()}"

      result = API.produce(client, topic_name, 0, [%{value: "test"}])

      # Either succeeds (auto-create enabled) or fails with error
      case result do
        {:ok, %Produce{}} -> assert true
        {:error, error} -> assert error in [:unknown_topic_or_partition, :leader_not_available]
      end
    end

    test "returns error for invalid partition", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name, partitions: 1)

      # Try to produce to partition 99 which doesn't exist
      {:error, error} = API.produce(client, topic_name, 99, [%{value: "test"}])

      assert error in [:unknown_topic_or_partition, :leader_not_available]
    end
  end

  describe "produce and fetch round-trip" do
    test "produced messages can be fetched", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce a message
      {:ok, produce_result} = API.produce(client, topic_name, 0, [%{value: "round trip test"}])
      offset = produce_result.base_offset

      # Fetch the message using legacy API
      fetch_response =
        KafkaEx.fetch(
          topic_name,
          0,
          offset: offset,
          worker_name: client
        )

      # Verify message was fetched
      assert [%{partitions: [%{message_set: messages}]}] = fetch_response
      assert length(messages) >= 1
      [message | _] = messages
      assert message.value == "round trip test"
    end

    test "produced messages with key can be fetched with key", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce a message with key
      {:ok, produce_result} =
        API.produce(client, topic_name, 0, [%{value: "keyed message", key: "my-key"}])

      offset = produce_result.base_offset

      # Fetch the message
      fetch_response =
        KafkaEx.fetch(
          topic_name,
          0,
          offset: offset,
          worker_name: client
        )

      assert [%{partitions: [%{message_set: messages}]}] = fetch_response
      [message | _] = messages
      assert message.value == "keyed message"
      assert message.key == "my-key"
    end

    test "multiple produced messages can be fetched in order", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Produce multiple messages
      {:ok, result1} = API.produce(client, topic_name, 0, [%{value: "first"}])
      {:ok, _result2} = API.produce(client, topic_name, 0, [%{value: "second"}])
      {:ok, _result3} = API.produce(client, topic_name, 0, [%{value: "third"}])

      # Fetch all messages
      fetch_response =
        KafkaEx.fetch(
          topic_name,
          0,
          offset: result1.base_offset,
          worker_name: client
        )

      assert [%{partitions: [%{message_set: messages}]}] = fetch_response
      assert length(messages) >= 3

      values = Enum.map(messages, & &1.value)
      assert "first" in values
      assert "second" in values
      assert "third" in values
    end
  end

  describe "produce with latest_offset verification" do
    test "latest_offset reflects produced messages", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Get initial offset
      {:ok, initial_offset} = API.latest_offset(client, topic_name, 0)

      # Produce messages
      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "msg1"}])
      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "msg2"}])
      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "msg3"}])

      # Get new offset
      {:ok, new_offset} = API.latest_offset(client, topic_name, 0)

      # Offset should have increased by 3
      assert new_offset == initial_offset + 3
    end
  end
end
