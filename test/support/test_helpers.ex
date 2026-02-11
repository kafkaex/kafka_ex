defmodule KafkaEx.TestHelpers do
  alias KafkaEx.API
  alias KafkaEx.Client
  alias KafkaEx.Client.NodeSelector
  require Logger

  @doc """
  Returns a random string of length string_length.
  """
  def generate_random_string(string_length \\ 20) do
    1..string_length
    |> Enum.map(fn _ -> round(:rand.uniform() * 25 + 65) end)
    |> to_string
  end

  @doc """
  Returns a random port number that is not in use.
  """
  def get_free_port(port) do
    case :gen_tcp.listen(port, [:binary]) do
      {:ok, socket} ->
        :ok = :gen_tcp.close(socket)
        port

      {:error, :eaddrinuse} ->
        get_free_port(port + 1)
    end
  end

  @doc """
  Wait for the return value of value_getter to pass the predicate condn
  ~> If condn does not pass, sleep for dwell msec and try again
  ~> If condn does not pass after max_tries attempts, raises an error
  """
  def wait_for_value(value_getter, condn, dwell \\ 500, max_tries \\ 200) do
    wait_for_value(value_getter, condn, dwell, max_tries, 0)
  end

  @doc """
  Wait for condn to return false or nil; passes through to wait_for_value
  returns :ok on success
  """
  def wait_for(condn, dwell \\ 500, max_tries \\ 200) do
    wait_for_value(fn -> :ok end, fn :ok -> condn.() end, dwell, max_tries)
  end

  @doc """
  Execute value_getter, which should return a list, and accumulate
  the results until the accumulated results are at least min_length long
  """
  def wait_for_accum(value_getter, min_length, dwell \\ 500, max_tries \\ 200) do
    wait_for_accum(value_getter, [], min_length, dwell, max_tries)
  end

  @doc """
  passthrough to wait_for_accum with 1 as the min_length - i.e.,
  wait for any response
  """
  def wait_for_any(value_getter, dwell \\ 500, max_tries \\ 200) do
    wait_for_accum(value_getter, 1, dwell, max_tries)
  end

  @doc """
  Returns a list of the brokers in the cluster
  """
  def uris do
    Application.get_env(:kafka_ex, :brokers)
  end

  def utc_time do
    {x, {a, b, c}} =
      :calendar.local_time()
      |> :calendar.local_time_to_universal_time_dst()
      |> hd

    {x, {a, b, c + 60}}
  end

  def latest_offset_number(topic, partition_id, worker \\ :kafka_ex) do
    case API.latest_offset(worker, topic, partition_id) do
      {:ok, offset} -> offset
      {:error, _} -> 0
    end
  end

  def latest_consumer_offset_number(
        topic,
        partition,
        consumer_group,
        worker \\ :kafka_ex,
        api_version \\ 0
      ) do
    partitions = [%{partition_num: partition}]

    case API.fetch_committed_offset(worker, consumer_group, topic, partitions, api_version: api_version) do
      {:ok, [%{partition_offsets: [%{offset: offset} | _]} | _]} -> offset
      {:ok, _} -> -1
      {:error, _} -> -1
    end
  end

  def ensure_append_timestamp_topic(client, topic_name) do
    resp =
      Client.send_request(
        client,
        %Kayrock.CreateTopics.V0.Request{
          topics: [
            %{
              name: topic_name,
              num_partitions: 4,
              replication_factor: 1,
              assignments: [],
              configs: [
                %{
                  name: "message.timestamp.type",
                  value: "LogAppendTime"
                }
              ]
            }
          ],
          timeout_ms: 1000
        },
        NodeSelector.controller()
      )

    {:ok,
     %Kayrock.CreateTopics.V0.Response{
       topics: [%{error_code: error_code}]
     }} = resp

    wait_for_topic_to_appear(client, topic_name)

    if error_code in [0, 36] do
      {:ok, topic_name}
    else
      Logger.error("Unable to create topic #{topic_name}: #{inspect(resp)}")
      {:error, topic_name}
    end
  end

  defp wait_for_topic_to_appear(_client, _topic_name, attempts \\ 10)

  defp wait_for_topic_to_appear(_client, _topic_name, attempts) when attempts <= 0 do
    raise "Timeout while waiting for topic to appear"
  end

  defp wait_for_topic_to_appear(client, topic_name, attempts) do
    {:ok, %{topics: topic_entries}} =
      Client.send_request(
        client,
        %Kayrock.Metadata.V0.Request{},
        NodeSelector.topic_partition(topic_name, 0)
      )

    topics = topic_entries |> Enum.map(& &1.name)

    unless topic_name in topics do
      wait_for_topic_to_appear(client, topic_name, attempts - 1)
    end
  end

  defp wait_for_value(_value_getter, _condn, _dwell, max_tries, n)
       when n >= max_tries do
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

  defp wait_for_accum(_value_getter, acc, min_length, _dwell, _max_tries)
       when length(acc) >= min_length do
    acc
  end

  defp wait_for_accum(value_getter, acc, min_length, dwell, max_tries) do
    value =
      wait_for_value(
        value_getter,
        fn
          {:ok, v} -> length(v) > 0
          v -> length(v) > 0
        end,
        dwell,
        max_tries
      )

    wait_for_accum(value_getter, acc ++ value, min_length, dwell, max_tries)
  end
end
