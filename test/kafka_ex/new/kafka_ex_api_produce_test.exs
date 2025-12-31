defmodule KafkaEx.APIProduceTest do
  use ExUnit.Case, async: false

  alias KafkaEx.API, as: KafkaExAPI
  alias KafkaEx.Messages.RecordMetadata

  # Mock GenServer for testing
  defmodule MockClient do
    use GenServer

    def start_link(initial_state) do
      GenServer.start_link(__MODULE__, initial_state)
    end

    def init(state) do
      {:ok, state}
    end

    def handle_call({:produce, topic, partition, messages, opts}, _from, state) do
      # Store the call args for test verification
      call_info = %{
        topic: topic,
        partition: partition,
        messages: messages,
        opts: opts
      }

      new_state = Map.put(state, :last_call, call_info)

      # Return configured response or default
      response = Map.get(state, :produce_response, {:ok, build_default_produce()})
      {:reply, response, new_state}
    end

    def handle_call(:get_last_call, _from, state) do
      {:reply, Map.get(state, :last_call), state}
    end

    defp build_default_produce do
      %RecordMetadata{
        topic: "test-topic",
        partition: 0,
        base_offset: 100,
        log_append_time: nil,
        throttle_time_ms: nil,
        log_start_offset: nil
      }
    end
  end

  describe "KafkaExAPI.produce/4" do
    test "produces messages with default options" do
      {:ok, client} = MockClient.start_link(%{})

      messages = [%{value: "hello"}, %{value: "world"}]
      {:ok, result} = KafkaExAPI.produce(client, "test-topic", 0, messages)

      assert %RecordMetadata{} = result
      assert result.base_offset == 100

      last_call = GenServer.call(client, :get_last_call)
      assert last_call.topic == "test-topic"
      assert last_call.partition == 0
      assert last_call.messages == messages
      assert last_call.opts == []
    end

    test "produces messages with keys" do
      {:ok, client} = MockClient.start_link(%{})

      messages = [
        %{value: "value1", key: "key1"},
        %{value: "value2", key: "key2"}
      ]

      {:ok, _result} = KafkaExAPI.produce(client, "test-topic", 0, messages)

      last_call = GenServer.call(client, :get_last_call)
      assert last_call.messages == messages
    end

    test "produces single message" do
      {:ok, client} = MockClient.start_link(%{})

      messages = [%{value: "single message"}]
      {:ok, result} = KafkaExAPI.produce(client, "my-topic", 1, messages)

      assert %RecordMetadata{} = result

      last_call = GenServer.call(client, :get_last_call)
      assert last_call.topic == "my-topic"
      assert last_call.partition == 1
      assert length(last_call.messages) == 1
    end
  end

  describe "KafkaExAPI.produce/5" do
    test "passes acks option" do
      {:ok, client} = MockClient.start_link(%{})

      messages = [%{value: "test"}]
      {:ok, _result} = KafkaExAPI.produce(client, "test-topic", 0, messages, acks: 1)

      last_call = GenServer.call(client, :get_last_call)
      assert Keyword.get(last_call.opts, :acks) == 1
    end

    test "passes timeout option" do
      {:ok, client} = MockClient.start_link(%{})

      messages = [%{value: "test"}]
      {:ok, _result} = KafkaExAPI.produce(client, "test-topic", 0, messages, timeout: 10_000)

      last_call = GenServer.call(client, :get_last_call)
      assert Keyword.get(last_call.opts, :timeout) == 10_000
    end

    test "passes compression option" do
      {:ok, client} = MockClient.start_link(%{})

      messages = [%{value: "test"}]
      {:ok, _result} = KafkaExAPI.produce(client, "test-topic", 0, messages, compression: :gzip)

      last_call = GenServer.call(client, :get_last_call)
      assert Keyword.get(last_call.opts, :compression) == :gzip
    end

    test "passes api_version option" do
      {:ok, client} = MockClient.start_link(%{})

      messages = [%{value: "test"}]
      {:ok, _result} = KafkaExAPI.produce(client, "test-topic", 0, messages, api_version: 3)

      last_call = GenServer.call(client, :get_last_call)
      assert Keyword.get(last_call.opts, :api_version) == 3
    end

    test "passes multiple options together" do
      {:ok, client} = MockClient.start_link(%{})

      messages = [%{value: "test"}]

      {:ok, _result} =
        KafkaExAPI.produce(client, "test-topic", 0, messages,
          acks: -1,
          timeout: 5000,
          compression: :snappy,
          api_version: 2
        )

      last_call = GenServer.call(client, :get_last_call)
      assert Keyword.get(last_call.opts, :acks) == -1
      assert Keyword.get(last_call.opts, :timeout) == 5000
      assert Keyword.get(last_call.opts, :compression) == :snappy
      assert Keyword.get(last_call.opts, :api_version) == 2
    end

    test "returns error when request fails with Error struct" do
      {:ok, client} =
        MockClient.start_link(%{
          produce_response: {:error, %{error: :unknown_topic_or_partition}}
        })

      messages = [%{value: "test"}]

      assert {:error, :unknown_topic_or_partition} =
               KafkaExAPI.produce(client, "bad-topic", 0, messages)
    end

    test "returns error when request fails with atom" do
      {:ok, client} =
        MockClient.start_link(%{
          produce_response: {:error, :timeout}
        })

      messages = [%{value: "test"}]
      assert {:error, :timeout} = KafkaExAPI.produce(client, "test-topic", 0, messages)
    end
  end

  describe "KafkaExAPI.produce/5 with response fields" do
    test "returns all response fields" do
      produce_response = %RecordMetadata{
        topic: "events",
        partition: 2,
        base_offset: 500,
        log_append_time: 1_702_000_000_000,
        throttle_time_ms: 10,
        log_start_offset: 100
      }

      {:ok, client} = MockClient.start_link(%{produce_response: {:ok, produce_response}})

      messages = [%{value: "test"}]
      {:ok, result} = KafkaExAPI.produce(client, "events", 2, messages)

      assert result.topic == "events"
      assert result.partition == 2
      assert result.base_offset == 500
      assert result.log_append_time == 1_702_000_000_000
      assert result.throttle_time_ms == 10
      assert result.log_start_offset == 100
    end

    test "handles nil optional fields" do
      produce_response = %RecordMetadata{
        topic: "test",
        partition: 0,
        base_offset: 0,
        log_append_time: nil,
        throttle_time_ms: nil,
        log_start_offset: nil
      }

      {:ok, client} = MockClient.start_link(%{produce_response: {:ok, produce_response}})

      messages = [%{value: "test"}]
      {:ok, result} = KafkaExAPI.produce(client, "test", 0, messages)

      assert result.log_append_time == nil
      assert result.throttle_time_ms == nil
      assert result.log_start_offset == nil
    end
  end

  describe "KafkaExAPI.produce_one/4" do
    test "produces single message" do
      {:ok, client} = MockClient.start_link(%{})

      {:ok, result} = KafkaExAPI.produce_one(client, "test-topic", 0, "hello world")

      assert %RecordMetadata{} = result

      last_call = GenServer.call(client, :get_last_call)
      assert last_call.topic == "test-topic"
      assert last_call.partition == 0
      assert [%{value: "hello world"}] = last_call.messages
    end

    test "message has only value field when no options" do
      {:ok, client} = MockClient.start_link(%{})

      {:ok, _result} = KafkaExAPI.produce_one(client, "test-topic", 0, "test value")

      last_call = GenServer.call(client, :get_last_call)
      [message] = last_call.messages
      assert message == %{value: "test value"}
      refute Map.has_key?(message, :key)
      refute Map.has_key?(message, :timestamp)
      refute Map.has_key?(message, :headers)
    end
  end

  describe "KafkaExAPI.produce_one/5" do
    test "passes key to message" do
      {:ok, client} = MockClient.start_link(%{})

      {:ok, _result} = KafkaExAPI.produce_one(client, "test-topic", 0, "value", key: "my-key")

      last_call = GenServer.call(client, :get_last_call)
      [message] = last_call.messages
      assert message.value == "value"
      assert message.key == "my-key"
    end

    test "passes timestamp to message" do
      {:ok, client} = MockClient.start_link(%{})

      timestamp = 1_702_000_000_000
      {:ok, _result} = KafkaExAPI.produce_one(client, "test-topic", 0, "value", timestamp: timestamp)

      last_call = GenServer.call(client, :get_last_call)
      [message] = last_call.messages
      assert message.timestamp == timestamp
    end

    test "passes headers to message" do
      {:ok, client} = MockClient.start_link(%{})

      headers = [{"content-type", "application/json"}, {"trace-id", "abc123"}]
      {:ok, _result} = KafkaExAPI.produce_one(client, "test-topic", 0, "value", headers: headers)

      last_call = GenServer.call(client, :get_last_call)
      [message] = last_call.messages
      assert message.headers == headers
    end

    test "separates message opts from produce opts" do
      {:ok, client} = MockClient.start_link(%{})

      {:ok, _result} =
        KafkaExAPI.produce_one(client, "test-topic", 0, "value",
          key: "my-key",
          timestamp: 1_702_000_000_000,
          headers: [{"x-header", "val"}],
          acks: 1,
          timeout: 10_000,
          compression: :gzip,
          api_version: 3
        )

      last_call = GenServer.call(client, :get_last_call)

      # Message should have message-specific fields
      [message] = last_call.messages
      assert message.value == "value"
      assert message.key == "my-key"
      assert message.timestamp == 1_702_000_000_000
      assert message.headers == [{"x-header", "val"}]

      # Produce opts should have produce-specific fields
      assert Keyword.get(last_call.opts, :acks) == 1
      assert Keyword.get(last_call.opts, :timeout) == 10_000
      assert Keyword.get(last_call.opts, :compression) == :gzip
      assert Keyword.get(last_call.opts, :api_version) == 3

      # Produce opts should NOT have message-specific fields
      refute Keyword.has_key?(last_call.opts, :key)
      refute Keyword.has_key?(last_call.opts, :timestamp)
      refute Keyword.has_key?(last_call.opts, :headers)
    end

    test "does not add nil keys to message" do
      {:ok, client} = MockClient.start_link(%{})

      {:ok, _result} = KafkaExAPI.produce_one(client, "test-topic", 0, "value", acks: 1)

      last_call = GenServer.call(client, :get_last_call)
      [message] = last_call.messages

      # Message should only have :value
      assert Map.keys(message) == [:value]
    end

    test "returns error when request fails" do
      {:ok, client} =
        MockClient.start_link(%{
          produce_response: {:error, :not_leader_for_partition}
        })

      assert {:error, :not_leader_for_partition} =
               KafkaExAPI.produce_one(client, "test-topic", 0, "value")
    end
  end

  describe "KafkaExAPI.produce_one/5 edge cases" do
    test "handles empty value" do
      {:ok, client} = MockClient.start_link(%{})

      {:ok, _result} = KafkaExAPI.produce_one(client, "test-topic", 0, "")

      last_call = GenServer.call(client, :get_last_call)
      [message] = last_call.messages
      assert message.value == ""
    end

    test "handles binary value with special characters" do
      {:ok, client} = MockClient.start_link(%{})

      binary_value = <<0, 1, 2, 255, 254, 253>>
      {:ok, _result} = KafkaExAPI.produce_one(client, "test-topic", 0, binary_value)

      last_call = GenServer.call(client, :get_last_call)
      [message] = last_call.messages
      assert message.value == binary_value
    end

    test "handles nil key explicitly passed" do
      {:ok, client} = MockClient.start_link(%{})

      {:ok, _result} = KafkaExAPI.produce_one(client, "test-topic", 0, "value", key: nil)

      last_call = GenServer.call(client, :get_last_call)
      [message] = last_call.messages
      # nil key should not be added to message
      refute Map.has_key?(message, :key)
    end

    test "handles empty headers list" do
      {:ok, client} = MockClient.start_link(%{})

      {:ok, _result} = KafkaExAPI.produce_one(client, "test-topic", 0, "value", headers: [])

      last_call = GenServer.call(client, :get_last_call)
      [message] = last_call.messages
      assert message.headers == []
    end
  end
end
