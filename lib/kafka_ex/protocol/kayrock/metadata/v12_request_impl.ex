defimpl KafkaEx.Protocol.Kayrock.Metadata.Request, for: Kayrock.Metadata.V12.Request do
  @moduledoc """
  Implementation of Metadata Request protocol for Kafka V12 API.

  V12 is the first version where brokers honour topic-id lookups. Named
  topics (`:topics`) are sent with the null topic id; ids passed in
  `:topic_ids` are sent as `%{topic_id: id, name: nil}`. Fields are otherwise
  the same as V11.
  """

  alias KafkaEx.Protocol.Kayrock.Metadata.RequestHelpers

  def build_request(request_template, opts) do
    RequestHelpers.build_v12_plus_request(request_template, opts)
  end
end
