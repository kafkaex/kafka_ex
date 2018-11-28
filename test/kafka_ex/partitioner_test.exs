defmodule KafkaEx.PartitionerTest do
  alias KafkaEx.Partitioner

  alias KafkaEx.Protocol.Produce.Request, as: ProduceRequest
  alias KafkaEx.Protocol.Produce.Message, as: ProduceMessage

  use ExUnit.Case

  @spec request(messages :: [{binary | nil, binary}]) :: ProduceRequest.t()
  def request(messages) do
    %ProduceRequest{
      topic: "test_topic",
      messages:
        Enum.map(messages, fn {key, value} ->
          %ProduceMessage{key: key, value: value}
        end)
    }
  end

  test "key detection" do
    assert {:ok, "key"} ==
             Partitioner.get_key(
               request([
                 {"key", "message_1"},
                 {"key", "message_2"},
                 {"key", "message_3"},
                 {"key", "message_3"}
               ])
             )

    assert {:error, :inconsistent_keys} ==
             Partitioner.get_key(
               request([
                 {"key", "message"},
                 {"key2", "message"}
               ])
             )

    assert {:error, :inconsistent_keys} ==
             Partitioner.get_key(
               request([
                 {"key", "message"},
                 {nil, "message"}
               ])
             )

    assert {:error, :inconsistent_keys} ==
             Partitioner.get_key(
               request([
                 {nil, "message"},
                 {"key", "message"}
               ])
             )

    assert {:ok, nil} ==
             Partitioner.get_key(
               request([
                 {nil, "message"},
                 {nil, "message"}
               ])
             )

    assert {:error, :no_messages} == Partitioner.get_key(request([]))
  end
end
