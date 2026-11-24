defmodule KafkaEx.Pins.TopicIdVersionPinTest do
  use KafkaEx.TestSupport.PinCase, async: false

  @moduletag pin_set: :invariant

  @id <<1::128>>

  defp start(topic_id) do
    start_pinned(
      api_versions: %{metadata: 10..10, offset_commit: 2..10, offset_fetch: 1..10},
      before_boot: fn fake ->
        FakeTransport.respond(fake, :metadata, fn request ->
          reply = metadata_reply(request.host, [{@topic, [1, 2]}])
          %{reply | topics: Enum.map(reply.topics, &Map.put(&1, :topic_id, topic_id))}
        end)
      end
    )
  end

  defp versions(fake, api), do: fake |> requests(api) |> Enum.map(& &1.api_version) |> Enum.uniq()

  defp commit(client) do
    GenServer.call(client, {:offset_commit, "g", [{@topic, [%{partition_num: 0, offset: 5}]}], []})
  end

  defp fetch(client), do: GenServer.call(client, {:offset_fetch, "g", [{@topic, [%{partition_num: 0}]}], []})

  for {name, call, api} <- [{"OffsetCommit", :commit, :offset_commit}, {"OffsetFetch", :fetch, :offset_fetch}] do
    test "#{name} v10 is sent as v10 when the topic id is in metadata" do
      %{fake: fake, client: client} = start(@id)
      with_log(fn -> unquote(call)(client) end)
      assert versions(fake, unquote(api)) == [10]
    end

    test "#{name} v10 is sent as v9, by name, when the topic id is not in metadata" do
      %{fake: fake, client: client} = start(<<0::128>>)
      with_log(fn -> unquote(call)(client) end)
      assert versions(fake, unquote(api)) == [9]
    end
  end
end
