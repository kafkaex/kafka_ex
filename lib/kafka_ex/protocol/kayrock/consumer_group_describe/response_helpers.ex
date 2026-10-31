defmodule KafkaEx.Protocol.Kayrock.ConsumerGroupDescribe.ResponseHelpers do
  @moduledoc """
  Shared helper functions for parsing ConsumerGroupDescribe responses (V0-V1).

  V1 adds `member_type` per member; `ConsumerGroupDescription.Member` reads it
  with `Map.get/2`, so it is `nil` for V0 responses.
  """

  alias KafkaEx.Messages.ConsumerGroupDescription
  alias KafkaEx.Protocol.Kayrock.DescribeGroups.ResponseHelpers, as: DescribeGroupsHelpers

  @doc """
  Returns `{:ok, [ConsumerGroupDescription.t()]}` when every group succeeded,
  or `{:error, [{group_id, error_atom}]}` for the failed groups (the
  DescribeGroups envelope, with the KIP-848 group builder).
  """
  @spec parse_response(map()) :: {:ok, [ConsumerGroupDescription.t()]} | {:error, [{String.t(), atom()}]}
  def parse_response(response) do
    DescribeGroupsHelpers.parse_response(response, &ConsumerGroupDescription.from_consumer_group_describe_response/1)
  end
end
