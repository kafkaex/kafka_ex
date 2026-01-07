defmodule KafkaEx.Protocol.Kayrock.FindCoordinator.RequestHelpers do
  @moduledoc """
  Shared utility functions for building FindCoordinator requests.
  """

  @doc """
  Extracts the group_id from options for V0 requests.
  """
  @spec extract_group_id(Keyword.t()) :: binary()
  def extract_group_id(opts) do
    Keyword.fetch!(opts, :group_id)
  end

  @doc """
  Extracts coordinator key and type from options for V1 requests.

  Returns `{coordinator_key, coordinator_type}` where:
  - `coordinator_key` is the group_id or transactional_id
  - `coordinator_type` is 0 (group) or 1 (transaction)
  """
  @spec extract_v1_fields(Keyword.t()) :: {binary(), 0 | 1}
  def extract_v1_fields(opts) do
    # V1 uses coordinator_key, but we also accept group_id for convenience
    coordinator_key =
      Keyword.get(opts, :coordinator_key) || Keyword.fetch!(opts, :group_id)

    # Default to group coordinator (0)
    coordinator_type = extract_coordinator_type(opts)

    {coordinator_key, coordinator_type}
  end

  @doc """
  Extracts and normalizes coordinator type from options.

  Accepts:
  - Integer: 0 or 1
  - Atom: :group or :transaction

  Returns integer (0 or 1).
  """
  @spec extract_coordinator_type(Keyword.t()) :: 0 | 1
  def extract_coordinator_type(opts) do
    case Keyword.get(opts, :coordinator_type, 0) do
      :group -> 0
      :transaction -> 1
      0 -> 0
      1 -> 1
    end
  end
end
