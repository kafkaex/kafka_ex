defmodule KafkaEx.Pins.Issue445PinTest do
  use KafkaEx.TestSupport.PinCase, async: false

  alias KafkaEx.Client.Error
  alias KafkaEx.TestSupport.FakeTransport.Wire

  @moduletag pin_set: :invariant

  setup do
    start_pinned(before_boot: &FakeTransport.respond(&1, :metadata, fn request -> answer(request) end))
  end

  test "a reply that arrives after its request timed out never answers a later request", %{fake: fake, client: client} do
    with_request_timeout(200)
    FakeTransport.hold(fake, :metadata)

    {timed_out, _log} = with_log(fn -> API.metadata(client, ["b"], []) end)
    assert {:error, %Error{error: :timeout}} = timed_out

    FakeTransport.release(fake, :metadata)

    assert {:ok, %{topics: topics}} = API.metadata(client, ["a"], [])
    assert Map.keys(topics) == ["a"]
  end

  test "a failed refresh answers each topic from its own entry only", %{fake: fake, client: client} do
    assert {:ok, [%{name: "b"}]} = API.topics_metadata(client, ["b"])
    FakeTransport.respond(fake, :metadata, :close)
    with_request_timeout(200)

    {results, _log} =
      with_log(fn -> {API.topics_metadata(client, ["a"]), API.topics_metadata(client, ["b"])} end)

    assert {{:ok, []}, {:ok, [%{name: "b"}]}} = results
  end

  @tag skip: "the client does not compare a reply's correlation id with its request's"
  test "a reply carrying the previous request's correlation id is not taken as the answer", %{
    fake: fake,
    client: client
  } do
    with_request_timeout(200)

    FakeTransport.respond(fake, :metadata, fn request ->
      {:frame, Wire.response(request.api_key, request.api_version, request.correlation_id - 1, answer(request, ["b"]))}
    end)

    {result, _log} = with_log(fn -> API.metadata(client, ["a"], []) end)

    sent = List.last(requests(fake, :metadata))
    assert %{action: :reply} = sent
    assert asked(sent) == ["a"]
    assert {:error, _} = result
  end

  defp answer(request), do: answer(request, asked(request))
  defp answer(%{host: host}, topics), do: Wire.metadata_fields([{1, host, 9092}], Map.new(topics, &{&1, [1]}))

  defp asked(%{frame: <<_::64, size::16, _::binary-size(size), 0::32>>}), do: []

  defp asked(%{
         frame: <<_::64, size::16, _::binary-size(size), 1::32, length::16, topic::binary-size(length)>>
       }),
       do: [topic]

  defp asked(%{frame: frame}), do: raise(ArgumentError, "unrecognised metadata frame: #{inspect(frame)}")
end
