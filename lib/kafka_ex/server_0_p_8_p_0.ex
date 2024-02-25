defmodule KafkaEx.Server0P8P0 do
  @moduledoc """
  Implements KafkaEx.Server behaviors for Kafka >= 0.8.0 < 0.8.2 API.
  """

  # these functions aren't implemented for 0.8.0
  @dialyzer [
    {:nowarn_function, kafka_server_heartbeat: 3},
    {:nowarn_function, kafka_server_sync_group: 3},
    {:nowarn_function, kafka_server_join_group: 3},
    {:nowarn_function, kafka_server_leave_group: 3},
    {:nowarn_function, kafka_server_update_consumer_metadata: 1},
    {:nowarn_function, kafka_server_consumer_group_metadata: 1},
    {:nowarn_function, kafka_server_consumer_group: 1},
    {:nowarn_function, kafka_server_offset_commit: 2},
    {:nowarn_function, kafka_server_offset_fetch: 2},
    {:nowarn_function, kafka_server_create_topics: 3},
    {:nowarn_function, kafka_server_delete_topics: 3},
    {:nowarn_function, kafka_server_api_versions: 1}
  ]

  use KafkaEx.Server
  alias KafkaEx.Protocol.Fetch

  def kafka_server_init([args]) do
    kafka_server_init([args, self()])
  end

  def kafka_server_init([args, name]) do
    # warn if ssl is configured
    if Keyword.get(args, :use_ssl) do
      Logger.warning(fn ->
        "KafkaEx is being configured to use ssl with a broker version that " <>
          "does not support ssl"
      end)
    end

    state = kafka_common_init(args, name)

    {:ok, state}
  end

  def start_link(args, name \\ __MODULE__)

  def start_link(args, :no_name) do
    GenServer.start_link(__MODULE__, [args])
  end

  def start_link(args, name) do
    GenServer.start_link(__MODULE__, [args, name], name: name)
  end

  def kafka_server_fetch(fetch_request, state) do
    {response, state} = fetch(fetch_request, state)

    {:reply, response, state}
  end

  def kafka_server_offset_fetch(_, _state),
    do: raise("Offset Fetch is not supported in 0.8.0 version of Kafka")

  def kafka_server_offset_commit(_, _state),
    do: raise("Offset Commit is not supported in 0.8.0 version of Kafka")

  def kafka_server_consumer_group(_state),
    do: raise("Consumer Group is not supported in 0.8.0 version of Kafka")

  def kafka_server_consumer_group_metadata(_state),
    do: raise("Consumer Group Metadata is not supported in 0.8.0 version of Kafka")

  def kafka_server_join_group(_, _, _state),
    do: raise("Join Group is not supported in 0.8.0 version of Kafka")

  def kafka_server_sync_group(_, _, _state),
    do: raise("Sync Group is not supported in 0.8.0 version of Kafka")

  def kafka_server_leave_group(_, _, _state),
    do: raise("Leave Group is not supported in 0.8.0 version of Kafka")

  def kafka_server_heartbeat(_, _, _state),
    do: raise("Heartbeat is not supported in 0.8.0 version of Kafka")

  def kafka_server_update_consumer_metadata(_state),
    do: raise("Consumer Group Metadata is not supported in 0.8.0 version of Kafka")

  def kafka_server_api_versions(_state),
    do: raise("ApiVersions is not supported in 0.8.0 version of Kafka")

  def kafka_server_create_topics(_, _, _state),
    do: raise("CreateTopic is not supported in 0.8.0 version of Kafka")

  def kafka_server_delete_topics(_, _, _state),
    do: raise("DeleteTopic is not supported in 0.8.0 version of Kafka")

  defp fetch(request, state) do
    case network_request(request, Fetch, state) do
      {{:error, error}, state_out} -> {error, state_out}
      {response, state_out} -> {response, state_out}
    end
  end
end
