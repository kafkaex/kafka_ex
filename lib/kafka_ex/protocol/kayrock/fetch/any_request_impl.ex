defimpl KafkaEx.Protocol.Kayrock.Fetch.Request, for: Any do
  @moduledoc """
  Fallback implementation of Fetch Request protocol for unknown future versions.

  All known Kayrock-supported versions (V0-V11) have explicit protocol
  implementations. This `Any` fallback exists solely for forward compatibility
  with potential future Kayrock versions that don't yet have an explicit impl.

  Detects the version range by checking for distinguishing struct fields:
  - `session_id` present: V7+ path (includes all V7+ features)
  - `max_bytes` present: V3+ path
  - Otherwise: V0-V2 path (basic fetch)
  """

  alias KafkaEx.Protocol.Kayrock.Fetch.RequestHelpers

  def build_request(request, opts) do
    cond do
      Map.has_key?(request, :session_id) ->
        # V7+ style: determine best version based on available fields
        api_version =
          cond do
            Map.has_key?(request, :rack_id) -> 11
            true -> 7
          end

        RequestHelpers.build_request_v7_plus(request, opts, api_version)

      Map.has_key?(request, :max_bytes) ->
        # V3-V6 style
        fields = RequestHelpers.extract_common_fields(opts)
        topics = RequestHelpers.build_topics(fields, Keyword.put(opts, :api_version, 5))

        request
        |> RequestHelpers.populate_request(fields, topics)
        |> RequestHelpers.add_max_bytes(fields, 3)
        |> RequestHelpers.add_isolation_level(opts, 4)

      true ->
        # V0-V2 style
        fields = RequestHelpers.extract_common_fields(opts)
        topics = RequestHelpers.build_topics(fields, Keyword.put(opts, :api_version, 0))

        RequestHelpers.populate_request(request, fields, topics)
    end
  end
end
