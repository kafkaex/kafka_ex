defimpl KafkaEx.New.Protocols.Kayrock.Metadata.Request, for: Kayrock.Metadata.V1.Request do
  @moduledoc """
  Implementation of Metadata Request protocol for Kafka V1 API.
  """

  alias KafkaEx.New.Protocols.Kayrock.Metadata.RequestHelpers

  @doc """
  Builds a V1 Metadata request.
  """
  def build_request(request_template, opts) do
    topics = RequestHelpers.build_topics_list(opts)
    Map.put(request_template, :topics, topics)
  end
end
