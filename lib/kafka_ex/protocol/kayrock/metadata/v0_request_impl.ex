defimpl KafkaEx.Protocol.Kayrock.Metadata.Request, for: Kayrock.Metadata.V0.Request do
  @moduledoc """
  Implementation of Metadata Request protocol for Kafka V0 API.
  """

  alias KafkaEx.Protocol.Kayrock.Metadata.RequestHelpers

  @doc """
  Builds a V0 Metadata request.
  """
  def build_request(request_template, opts) do
    topics = RequestHelpers.build_topics_list(opts) || []
    Map.put(request_template, :topics, topics)
  end
end
