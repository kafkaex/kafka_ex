defimpl KafkaEx.New.Protocols.Kayrock.Fetch.Request, for: Kayrock.Fetch.V4.Request do
  alias KafkaEx.New.Protocols.Kayrock.Fetch.RequestHelpers

  def build_request(request, opts) do
    fields = RequestHelpers.extract_common_fields(opts)
    topics = RequestHelpers.build_topics(fields, Keyword.put(opts, :api_version, 4))

    request
    |> RequestHelpers.populate_request(fields, topics)
    |> RequestHelpers.add_max_bytes(fields, 4)
    |> RequestHelpers.add_isolation_level(opts, 4)
  end
end
