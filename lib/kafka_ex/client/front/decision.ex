defmodule KafkaEx.Client.Front.Decision do
  @moduledoc false

  alias KafkaEx.Client.Error
  alias KafkaEx.Client.Front.Policy
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Support.Retry
  alias Kayrock.ErrorCode

  @type branch :: :send_once | :unknown_topic_id | :retry | :exhausted | :not_retryable | :replica
  @type next :: :reply | :park_metadata | :park_coordinator | :backoff
  @type refusal :: :reply | :next_candidate | :leader | :park_coordinator | :park_connection

  @spec written(Policy.t(), pos_integer(), atom()) :: {next(), branch(), [ClusterMetadata.action()]}
  def written(%Policy{mode: mode}, _spent, _error) when mode in [:send_request, :acks0] do
    {:reply, :send_once, []}
  end

  def written(%Policy{} = policy, spent, error) do
    cond do
      policy.send_once?.(error) and Policy.coordinator?(policy) ->
        {:reply, :send_once, actions(policy, error)}

      error == :unknown_topic_id ->
        {:reply, :unknown_topic_id, actions(policy, error)}

      policy.retryable?.(error) and spent < policy.max_attempts ->
        {retry(policy, error), :retry, actions(policy, error)}

      policy.retryable?.(error) ->
        {:reply, :exhausted, actions(policy, error)}

      true ->
        {:reply, :not_retryable, []}
    end
  end

  @spec refused(Policy.t(), boolean()) :: {refusal(), [ClusterMetadata.action()]}
  def refused(%Policy{mode: :acks0}, _via_replica?), do: {:reply, []}

  def refused(%Policy{} = policy, via_replica?) do
    cond do
      Policy.first_available?(policy) -> {:next_candidate, []}
      via_replica? -> {:leader, []}
      policy.rediscover_coordinator? -> {:park_coordinator, actions(policy, :not_connected)}
      true -> {:park_connection, []}
    end
  end

  @spec miss_actions(Policy.t()) :: [ClusterMetadata.action()]
  def miss_actions(%Policy{} = policy) do
    ClusterMetadata.selection_miss_actions(policy.selector,
      allow_auto_topic_creation: policy.allow_auto_topic_creation
    )
  end

  # A broker answer is parsed, a Kafka error code or :parse_error; anything else is a connection failure.
  @spec transport_failure?(term()) :: boolean()
  def transport_failure?(%Error{}), do: false
  def transport_failure?({_group_id, error}) when is_atom(error), do: false
  def transport_failure?(:parse_error), do: false
  def transport_failure?(error) when is_atom(error), do: ErrorCode.atom_to_code(error) == nil
  def transport_failure?(_error), do: true

  @spec error_atom(term()) :: atom()
  def error_atom(%Error{error: error}), do: error
  def error_atom(error) when is_atom(error), do: error
  def error_atom({_group_id, error}) when is_atom(error), do: error
  # Every parsed answer matches a clause above, so anything else is a connection failure and retries as one.
  def error_atom(_transport_reason), do: :transport_error

  defp retry(policy, error) do
    cond do
      Retry.leadership_error?(error) and Policy.partition?(policy) -> :park_metadata
      Policy.coordinator?(policy) and Retry.coordinator_refresh_error?(error) -> :park_coordinator
      true -> :backoff
    end
  end

  defp actions(%Policy{} = policy, error) do
    ClusterMetadata.error_actions(error, policy.selector,
      rediscover_coordinator?: policy.rediscover_coordinator?,
      allow_auto_topic_creation: policy.allow_auto_topic_creation
    )
  end
end
