defmodule KafkaEx.Protocol.Kayrock.ConsumerGroupDescribe.RequestHelpers do
  @moduledoc """
  Shared helper functions for building ConsumerGroupDescribe requests (V0-V1).

  V0 and V1 requests are schema-identical (`group_ids`,
  `include_authorized_operations`); V1 only changes the response.
  """

  @doc """
  Builds a ConsumerGroupDescribe request from the template.

  ## Options
  - `group_ids` (required): List of consumer group IDs to describe
  - `include_authorized_operations` (optional): defaults to `false`
  """
  @spec build_request_from_template(map(), Keyword.t()) :: map()
  def build_request_from_template(request_template, opts) do
    %{
      request_template
      | group_ids: Keyword.fetch!(opts, :group_ids),
        include_authorized_operations: Keyword.get(opts, :include_authorized_operations, false)
    }
  end
end
