defimpl KafkaEx.Protocol.Kayrock.Fetch.Request, for: Kayrock.Fetch.V2.Request do
  alias KafkaEx.Protocol.Kayrock.Fetch.RequestHelpers

  def build_request(request, opts) do
    fields = RequestHelpers.extract_common_fields(opts)
    topics = RequestHelpers.build_topics(fields, Keyword.put(opts, :api_version, 2))

    RequestHelpers.populate_request(request, fields, topics)
  end
end
