defimpl KafkaEx.New.Protocols.Kayrock.Fetch.Request, for: Kayrock.Fetch.V3.Request do
  alias KafkaEx.New.Protocols.Kayrock.Fetch.RequestHelpers

  def build_request(request, opts) do
    fields = RequestHelpers.extract_common_fields(opts)
    topics = RequestHelpers.build_topics(fields, Keyword.put(opts, :api_version, 3))

    request
    |> RequestHelpers.populate_request(fields, topics)
    |> RequestHelpers.add_max_bytes(fields, 3)
  end
end
