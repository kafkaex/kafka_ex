defmodule KafkaEx.Protocol.Kayrock.JoinGroup.RequestHelpers do
  @moduledoc """
  Shared helper functions for building JoinGroup requests across all versions.
  """

  @doc """
  Extracts common fields from request options.
  These fields are present in all versions (V0, V1, V2).

  If `group_protocols` is not provided but `topics` is, builds group_protocols
  automatically using the "assign" protocol with GroupProtocolMetadata.
  """
  @spec extract_common_fields(Keyword.t()) :: %{
          group_id: String.t(),
          session_timeout: integer(),
          member_id: String.t(),
          protocol_type: String.t(),
          group_protocols: [map()]
        }
  def extract_common_fields(opts) do
    group_protocols = get_or_build_group_protocols(opts)

    %{
      group_id: Keyword.fetch!(opts, :group_id),
      session_timeout: Keyword.fetch!(opts, :session_timeout),
      member_id: Keyword.fetch!(opts, :member_id),
      protocol_type: Keyword.get(opts, :protocol_type, "consumer"),
      group_protocols: group_protocols
    }
  end

  # Returns group_protocols if provided, otherwise builds from topics
  defp get_or_build_group_protocols(opts) do
    case Keyword.get(opts, :group_protocols) do
      nil ->
        topics = Keyword.fetch!(opts, :topics)
        build_group_protocols(topics)

      protocols ->
        protocols
    end
  end

  @doc """
  Builds group_protocols from a list of topics using the "assign" protocol.
  """
  @spec build_group_protocols([String.t()]) :: [map()]
  def build_group_protocols(topics) do
    [
      %{
        name: "assign",
        metadata: %Kayrock.GroupProtocolMetadata{topics: topics}
      }
    ]
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
    |> Map.put(:session_timeout_ms, session_timeout)
    |> Map.put(:member_id, member_id)
    |> Map.put(:protocol_type, protocol_type)
    |> Map.put(:protocols, group_protocols)
  end

  @doc """
  Builds a JoinGroup V1/V2 request (adds rebalance_timeout).
  """
  @spec build_v1_or_v2_request(map(), Keyword.t()) :: map()
  def build_v1_or_v2_request(request_template, opts) do
    rebalance_timeout = Keyword.fetch!(opts, :rebalance_timeout)

    request_template
    |> build_v0_request(opts)
    |> Map.put(:rebalance_timeout_ms, rebalance_timeout)
  end

  @doc """
  Builds a JoinGroup V5+ request (adds group_instance_id for static membership, KIP-345).

  The `group_instance_id` is used for static membership:
  - `nil` (default): Dynamic membership (same as V1-V4)
  - A string value: Static membership — the member keeps its assignment across restarts
  """
  @spec build_v5_plus_request(map(), Keyword.t()) :: map()
  def build_v5_plus_request(request_template, opts) do
    group_instance_id = Keyword.get(opts, :group_instance_id, nil)

    request_template
    |> build_v1_or_v2_request(opts)
    |> Map.put(:group_instance_id, group_instance_id)
  end
end
