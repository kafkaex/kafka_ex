defmodule KafkaEx.Protocol.Kayrock.DeleteTopics.RequestHelpers do
  @moduledoc """
  Shared helper functions for building DeleteTopics requests across all versions.
  """

  @doc """
  Extracts common fields from request options.

  ## Required Options

  - `:topics` - List of topic names to delete
  - `:timeout` - Request timeout in milliseconds

  ## Returns

  A map with extracted fields.
  """
  @spec extract_common_fields(Keyword.t()) :: map()
  def extract_common_fields(opts) do
    %{
      topics: Keyword.fetch!(opts, :topics),
      timeout: Keyword.fetch!(opts, :timeout)
    }
  end

  @doc """
  Builds a DeleteTopics request from a template struct and options.

  Populates `topic_names` and `timeout_ms` on the template from the
  `:topics` and `:timeout` options. Works for all versions (V0-V4)
  since the request schema only differs in encoding (handled by Kayrock).
  """
  @spec build_request_from_template(map(), Keyword.t()) :: map()
  def build_request_from_template(request_template, opts) do
    fields = extract_common_fields(opts)

    %{request_template | topic_names: fields.topics, timeout_ms: fields.timeout}
  end
end
