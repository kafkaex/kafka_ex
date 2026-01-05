defmodule KafkaEx.Messages.FindCoordinator do
  @moduledoc """
  This module represents FindCoordinator response from Kafka.

  FindCoordinator is used to discover the coordinator broker for a consumer group
  or transactional producer.

  ## Coordinator Types

  - `0` (`:group`) - Consumer group coordinator
  - `1` (`:transaction`) - Transaction coordinator

  ## API Version Differences

  - **V0**: Uses `group_id` parameter, returns coordinator info with `error_code`
  - **V1**: Uses `coordinator_key` + `coordinator_type`, adds `throttle_time_ms` and `error_message`
  """

  alias KafkaEx.Cluster.Broker

  defstruct [
    :coordinator,
    :error_code,
    :error_message,
    :throttle_time_ms
  ]

  @type coordinator_type :: :group | :transaction
  @type t :: %__MODULE__{
          coordinator: Broker.t() | nil,
          error_code: atom(),
          error_message: binary() | nil,
          throttle_time_ms: non_neg_integer() | nil
        }

  @doc """
  Builds a FindCoordinator struct from response data.
  """
  @spec build(Keyword.t()) :: t()
  def build(opts \\ []) do
    %__MODULE__{
      coordinator: Keyword.get(opts, :coordinator),
      error_code: Keyword.get(opts, :error_code, :no_error),
      error_message: Keyword.get(opts, :error_message),
      throttle_time_ms: Keyword.get(opts, :throttle_time_ms)
    }
  end

  @doc """
  Converts a coordinator type atom to its integer value.
  """
  @spec coordinator_type_to_int(coordinator_type()) :: 0 | 1
  def coordinator_type_to_int(:group), do: 0
  def coordinator_type_to_int(:transaction), do: 1

  @doc """
  Converts an integer coordinator type to its atom representation.
  """
  @spec int_to_coordinator_type(0 | 1) :: coordinator_type()
  def int_to_coordinator_type(0), do: :group
  def int_to_coordinator_type(1), do: :transaction

  @doc """
  Returns true if the response indicates success (no error).
  """
  @spec success?(t()) :: boolean()
  def success?(%__MODULE__{error_code: :no_error}), do: true
  def success?(%__MODULE__{}), do: false

  @doc """
  Returns the node_id of the coordinator if successful, nil otherwise.
  """
  @spec coordinator_node_id(t()) :: non_neg_integer() | nil
  def coordinator_node_id(%__MODULE__{coordinator: nil}), do: nil
  def coordinator_node_id(%__MODULE__{coordinator: coordinator}), do: coordinator.node_id
end
