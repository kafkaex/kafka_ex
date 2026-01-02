defmodule KafkaEx.Test.MockClient do
  @moduledoc """
  Mock client for testing KafkaEx.API functions without a real Kafka connection.

  ## Usage

      {:ok, client} = MockClient.start_link(%{
        produce: {:ok, %RecordMetadata{...}},
        fetch: {:ok, %Fetch{...}}
      })

      # Now use client with KafkaEx.API functions
      KafkaEx.API.produce(client, "topic", 0, [%{value: "hello"}])

      # Check what calls were made
      calls = MockClient.get_calls(client)
  """

  use GenServer

  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Messages.ApiVersions
  alias KafkaEx.Messages.CreateTopics
  alias KafkaEx.Messages.DeleteTopics
  alias KafkaEx.Messages.Fetch
  alias KafkaEx.Messages.FindCoordinator
  alias KafkaEx.Messages.Heartbeat
  alias KafkaEx.Messages.JoinGroup
  alias KafkaEx.Messages.RecordMetadata
  alias KafkaEx.Messages.SyncGroup

  def start_link(responses \\ %{}) do
    GenServer.start_link(__MODULE__, responses)
  end

  def init(responses), do: {:ok, %{responses: responses, calls: []}}

  def handle_call({:list_offsets, topic_partitions, _opts}, _from, state) do
    response = Map.get(state.responses, :list_offsets, {:ok, []})
    {:reply, response, record_call(state, {:list_offsets, topic_partitions})}
  end

  def handle_call({:metadata, topics, _opts, _api_version}, _from, state) do
    response = Map.get(state.responses, :metadata, {:ok, %ClusterMetadata{}})
    {:reply, response, record_call(state, {:metadata, topics})}
  end

  def handle_call(:cluster_metadata, _from, state) do
    response = Map.get(state.responses, :cluster_metadata, {:ok, %ClusterMetadata{}})
    {:reply, response, record_call(state, :cluster_metadata)}
  end

  def handle_call({:topic_metadata, topics, _allow_creation}, _from, state) do
    response = Map.get(state.responses, :topic_metadata, {:ok, []})
    {:reply, response, record_call(state, {:topic_metadata, topics})}
  end

  def handle_call({:api_versions, _opts}, _from, state) do
    response = Map.get(state.responses, :api_versions, {:ok, %ApiVersions{}})
    {:reply, response, record_call(state, :api_versions)}
  end

  def handle_call(:correlation_id, _from, state) do
    response = Map.get(state.responses, :correlation_id, {:ok, 1})
    {:reply, response, record_call(state, :correlation_id)}
  end

  def handle_call({:produce, topic, partition, messages, _opts}, _from, state) do
    response = Map.get(state.responses, :produce, {:ok, %RecordMetadata{}})
    {:reply, response, record_call(state, {:produce, topic, partition, messages})}
  end

  def handle_call({:fetch, topic, partition, offset, _opts}, _from, state) do
    response = Map.get(state.responses, :fetch, {:ok, %Fetch{}})
    {:reply, response, record_call(state, {:fetch, topic, partition, offset})}
  end

  def handle_call({:describe_groups, groups, _opts}, _from, state) do
    response = Map.get(state.responses, :describe_groups, {:ok, []})
    {:reply, response, record_call(state, {:describe_groups, groups})}
  end

  def handle_call({:join_group, group, member_id, _opts}, _from, state) do
    response = Map.get(state.responses, :join_group, {:ok, %JoinGroup{}})
    {:reply, response, record_call(state, {:join_group, group, member_id})}
  end

  def handle_call({:sync_group, group, gen_id, member_id, _opts}, _from, state) do
    response = Map.get(state.responses, :sync_group, {:ok, %SyncGroup{}})
    {:reply, response, record_call(state, {:sync_group, group, gen_id, member_id})}
  end

  def handle_call({:leave_group, group, member_id, _opts}, _from, state) do
    response = Map.get(state.responses, :leave_group, {:ok, :no_error})
    {:reply, response, record_call(state, {:leave_group, group, member_id})}
  end

  def handle_call({:heartbeat, group, member_id, gen_id, _opts}, _from, state) do
    response = Map.get(state.responses, :heartbeat, {:ok, %Heartbeat{}})
    {:reply, response, record_call(state, {:heartbeat, group, member_id, gen_id})}
  end

  def handle_call({:find_coordinator, group_id, _opts}, _from, state) do
    response = Map.get(state.responses, :find_coordinator, {:ok, %FindCoordinator{}})
    {:reply, response, record_call(state, {:find_coordinator, group_id})}
  end

  def handle_call({:offset_fetch, group, topic_partitions, _opts}, _from, state) do
    response = Map.get(state.responses, :offset_fetch, {:ok, []})
    {:reply, response, record_call(state, {:offset_fetch, group, topic_partitions})}
  end

  def handle_call({:offset_commit, group, topic_partitions, _opts}, _from, state) do
    response = Map.get(state.responses, :offset_commit, {:ok, []})
    {:reply, response, record_call(state, {:offset_commit, group, topic_partitions})}
  end

  def handle_call({:create_topics, topics, timeout, _opts}, _from, state) do
    response = Map.get(state.responses, :create_topics, {:ok, %CreateTopics{}})
    {:reply, response, record_call(state, {:create_topics, topics, timeout})}
  end

  def handle_call({:delete_topics, topics, timeout, _opts}, _from, state) do
    response = Map.get(state.responses, :delete_topics, {:ok, %DeleteTopics{}})
    {:reply, response, record_call(state, {:delete_topics, topics, timeout})}
  end

  def handle_call({:set_consumer_group_for_auto_commit, group}, _from, state) do
    response = Map.get(state.responses, :set_consumer_group, :ok)
    {:reply, response, record_call(state, {:set_consumer_group, group})}
  end

  def handle_call(:get_calls, _from, state) do
    {:reply, Enum.reverse(state.calls), state}
  end

  defp record_call(state, call) do
    %{state | calls: [call | state.calls]}
  end

  @doc """
  Returns the list of calls made to the mock client in order.
  """
  def get_calls(pid), do: GenServer.call(pid, :get_calls)
end
