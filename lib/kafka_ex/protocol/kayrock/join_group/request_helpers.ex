defmodule KafkaEx.Protocol.Kayrock.JoinGroup.RequestHelpers do
  @moduledoc """
  Shared helper functions for building JoinGroup requests across all versions.
  """

  @doc """
  Extracts common fields from request options.
  These fields are present in all versions (V0, V1, V2).
  """
  @spec extract_common_fields(Keyword.t()) :: %{
          group_id: String.t(),
          session_timeout: integer(),
          member_id: String.t(),
          protocol_type: String.t(),
          group_protocols: [map()]
        }
  def extract_common_fields(opts) do
    %{
      group_id: Keyword.fetch!(opts, :group_id),
      session_timeout: Keyword.fetch!(opts, :session_timeout),
      member_id: Keyword.fetch!(opts, :member_id),
      protocol_type: Keyword.get(opts, :protocol_type, "consumer"),
      group_protocols: Keyword.fetch!(opts, :group_protocols)
    }
  end

  @doc """
  Builds a JoinGroup V0 request by populating the request template with extracted fields.
  """
  @spec build_v0_request(map(), Keyword.t()) :: map()
  def build_v0_request(request_template, opts) do
    %{
      group_id: group_id,
      session_timeout: session_timeout,
      member_id: member_id,
      protocol_type: protocol_type,
      group_protocols: group_protocols
    } = extract_common_fields(opts)

    request_template
    |> Map.put(:group_id, group_id)
    |> Map.put(:session_timeout, session_timeout)
    |> Map.put(:member_id, member_id)
    |> Map.put(:protocol_type, protocol_type)
    |> Map.put(:group_protocols, group_protocols)
  end

  @doc """
  Builds a JoinGroup V1/V2 request (adds rebalance_timeout).
  """
  @spec build_v1_or_v2_request(map(), Keyword.t()) :: map()
  def build_v1_or_v2_request(request_template, opts) do
    rebalance_timeout = Keyword.fetch!(opts, :rebalance_timeout)

    request_template
    |> build_v0_request(opts)
    |> Map.put(:rebalance_timeout, rebalance_timeout)
  end
end
