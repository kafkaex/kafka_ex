defimpl KafkaEx.Protocol.Kayrock.Fetch.Request, for: Kayrock.Fetch.V6.Request do
  alias KafkaEx.Protocol.Kayrock.Fetch.RequestHelpers

  def build_request(request, opts) do
    fields = RequestHelpers.extract_common_fields(opts)
    topics = RequestHelpers.build_topics(fields, Keyword.put(opts, :api_version, 6))

    request
    |> RequestHelpers.populate_request(fields, topics)
    |> RequestHelpers.add_max_bytes(fields, 6)
    |> RequestHelpers.add_isolation_level(opts, 6)
  end
end
