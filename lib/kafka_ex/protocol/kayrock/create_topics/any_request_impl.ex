defimpl KafkaEx.Protocol.Kayrock.CreateTopics.Request, for: Any do
  @moduledoc """
  Fallback implementation of CreateTopics Request protocol for unknown future versions.

  All known Kayrock-supported versions (V0-V5) have explicit protocol
  implementations. This `Any` fallback exists solely for forward compatibility
  with potential future Kayrock versions that don't yet have an explicit impl.

  Detection logic based on struct fields:
  - Has `validate_only` key: V1+ path (build_v1_plus_request)
  - Otherwise: V0 path (inline logic with no validate_only)
  """

  alias KafkaEx.Protocol.Kayrock.CreateTopics.RequestHelpers

  def build_request(request_template, opts) do
    if Map.has_key?(request_template, :validate_only) do
      RequestHelpers.build_v1_plus_request(request_template, opts)
    else
      # V0 path: no validate_only field
      %{topics: topics, timeout: timeout} = RequestHelpers.extract_common_fields(opts)

      create_topic_requests = Enum.map(topics, &RequestHelpers.build_topic_request/1)

      request_template
      |> Map.put(:topics, create_topic_requests)
      |> Map.put(:timeout_ms, timeout)
    end
  end
end
