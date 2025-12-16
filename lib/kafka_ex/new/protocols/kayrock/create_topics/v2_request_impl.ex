defimpl KafkaEx.New.Protocols.Kayrock.CreateTopics.Request, for: Kayrock.CreateTopics.V2.Request do
  @moduledoc """
  Implementation for CreateTopics V2 Request.

  V2 schema is identical to V1:
  - create_topic_requests: array of topic configs
  - timeout: int32
  - validate_only: boolean

  The difference is in the response format (V2 adds throttle_time_ms).
  """

  alias KafkaEx.New.Protocols.Kayrock.CreateTopics.RequestHelpers

  def build_request(request_template, opts) do
    %{topics: topics, timeout: timeout} = RequestHelpers.extract_common_fields(opts)
    validate_only = Keyword.get(opts, :validate_only, false)

    create_topic_requests = Enum.map(topics, &RequestHelpers.build_topic_request/1)

    request_template
    |> Map.put(:create_topic_requests, create_topic_requests)
    |> Map.put(:timeout, timeout)
    |> Map.put(:validate_only, validate_only)
  end
end
