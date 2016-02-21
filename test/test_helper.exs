ExUnit.start()

ExUnit.configure exclude: [integration: true, consumer_group: true]

defmodule TestHelper do
  def generate_random_string(string_length \\ 20) do
    :random.seed(:os.timestamp)
    Enum.map(1..string_length, fn _ -> (:random.uniform * 25 + 65) |> round end) |> to_string
  end

  # Wait for the return value of value_getter to pass the predicate condn
  # If condn does not pass, sleep for dwell msec and try again
  # If condn does not pass after max_tries attempts, raises an error
  def wait_for_value(value_getter, condn, dwell \\ 500, max_tries \\ 10) do
    wait_for_value(value_getter, condn, dwell, max_tries, 0)
  end

  def uris do
    Application.get_env(:kafka_ex, :brokers)
  end

  def utc_time do
    {x, {a,b,c}} = :calendar.local_time |> :calendar.local_time_to_universal_time_dst |> hd
    {x, {a,b,c + 60}}
  end

  def latest_offset_number(topic, partition_id, worker \\ KafkaEx.Server) do
    offset = KafkaEx.latest_offset(topic, partition_id, worker)
      |> first_partition_offset

    offset || 0
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

  defp wait_for_value(value_getter, condn, dwell, max_tries, n) when n >= max_tries do
    raise "too many tries waiting for condition"
  end
  defp wait_for_value(value_getter, condn, dwell, max_tries, n) do
    value = value_getter.()
    if condn.(value) do
      value
    else
      :timer.sleep(dwell)
      wait_for_value(value_getter, condn, dwell, max_tries, n + 1)
    end
  end
end
