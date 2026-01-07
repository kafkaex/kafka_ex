defimpl KafkaEx.Protocol.Kayrock.Fetch.Request, for: Kayrock.Fetch.V7.Request do
  alias KafkaEx.Protocol.Kayrock.Fetch.RequestHelpers

  def build_request(request, opts) do
    fields = RequestHelpers.extract_common_fields(opts)
    topics = RequestHelpers.build_topics(fields, Keyword.put(opts, :api_version, 7))

    request
    |> RequestHelpers.populate_request(fields, topics)
    |> RequestHelpers.add_max_bytes(fields, 7)
    |> RequestHelpers.add_isolation_level(opts, 7)
    |> RequestHelpers.add_session_fields(opts, 7)
  end
end
