defimpl KafkaEx.New.Protocols.Kayrock.Metadata.Request, for: Kayrock.Metadata.V2.Request do
  @moduledoc """
  Implementation of Metadata Request protocol for Kafka V2 API.
  """

  alias KafkaEx.New.Protocols.Kayrock.Metadata.RequestHelpers

  @doc """
  Builds a V2 Metadata request.
  """
  def build_request(request_template, opts) do
    topics = RequestHelpers.build_topics_list(opts)

    request_template
    |> Map.put(:topics, topics)
  end
end
