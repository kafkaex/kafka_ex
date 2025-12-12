defimpl KafkaEx.New.Protocols.Kayrock.Fetch.Request, for: Kayrock.Fetch.V0.Request do
  alias KafkaEx.New.Protocols.Kayrock.Fetch.RequestHelpers

  def build_request(request, opts) do
    fields = RequestHelpers.extract_common_fields(opts)
    topics = RequestHelpers.build_topics(fields, Keyword.put(opts, :api_version, 0))

    RequestHelpers.populate_request(request, fields, topics)
  end
end
