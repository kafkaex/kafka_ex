ExUnit.start()

ExUnit.configure exclude: [integration: true, consumer_group: true]

defmodule TestHelper do
  def generate_random_string(string_length \\ 20) do
    :random.seed(:os.timestamp)
    Enum.map(1..string_length, fn _ -> (:random.uniform * 25 + 65) |> round end) |> to_string
  end

  def uris do
    Application.get_env(:kafka_ex, :brokers)
  end

  def utc_time do
    {x, {a,b,c}} = :calendar.local_time |> :calendar.local_time_to_universal_time_dst |> hd
    {x, {a,b,c + 60}}
  end

  def latest_offset_number(topic, partition_id, worker \\ KafkaEx.Server) do
    KafkaEx.latest_offset(topic, partition_id, worker)
    |> first_partition_offset
  end

  defp first_partition_offset(:topic_not_found) do
    nil
  end
  defp first_partition_offset(response) do
    [%KafkaEx.Protocol.Offset.Response{partition_offsets: partition_offsets}] =
      response
    first_partition = hd(partition_offsets)
    first_partition.offset |> hd
  end
end
