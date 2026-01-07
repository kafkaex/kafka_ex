defimpl KafkaEx.Protocol.Kayrock.CreateTopics.Request, for: Kayrock.CreateTopics.V0.Request do
  @moduledoc """
  Implementation for CreateTopics V0 Request.

  V0 schema:
  - create_topic_requests: array of topic configs
  - timeout: int32
  """

  alias KafkaEx.Protocol.Kayrock.CreateTopics.RequestHelpers

  def build_request(request_template, opts) do
    %{topics: topics, timeout: timeout} = RequestHelpers.extract_common_fields(opts)

    create_topic_requests = Enum.map(topics, &RequestHelpers.build_topic_request/1)

    request_template
    |> Map.put(:create_topic_requests, create_topic_requests)
    |> Map.put(:timeout, timeout)
  end
end
