defmodule KafkaEx.API.MetadataTopicListTest do
  use KafkaEx.TestSupport.PinCase, async: false

  defmodule Bound do
    @moduledoc false
    use KafkaEx.API

    def client, do: Process.get(:kafka_client)
  end

  setup do
    pinned = start_pinned(api_versions: %{metadata: 1..2}, topics: %{"a" => [1], "b" => [2]})
    Process.put(:kafka_client, pinned.client)
    {:ok, pinned}
  end

  defp requested_topics(fake, booted) do
    fake |> requests(:metadata) |> Enum.drop(booted) |> Enum.map(&decode_topics(&1.frame))
  end

  defp decode_topics(frame) do
    <<_api::16, _version::16, _corr::32, client_id_size::16-signed, _client_id::binary-size(client_id_size),
      count::32-signed, rest::binary>> = frame

    if count == -1, do: :all, else: decode_names(count, rest, [])
  end

  defp decode_names(0, _rest, acc), do: Enum.reverse(acc)

  defp decode_names(count, <<size::16, name::binary-size(size), rest::binary>>, acc),
    do: decode_names(count - 1, rest, [name | acc])

  describe "API.metadata/2" do
    test "a list of topic names requests only those topics", %{fake: fake, client: client} do
      booted = count(fake, :metadata)

      assert {:ok, _} = API.metadata(client, ["a"])

      assert requested_topics(fake, booted) == [["a"]]
    end

    test "keyword options still request all topics", %{fake: fake, client: client} do
      booted = count(fake, :metadata)

      assert {:ok, _} = API.metadata(client, api_version: 2)

      assert requested_topics(fake, booted) == [:all]
    end

    test "an empty list still requests all topics", %{fake: fake, client: client} do
      booted = count(fake, :metadata)

      assert {:ok, _} = API.metadata(client, [])

      assert requested_topics(fake, booted) == [:all]
    end
  end

  describe "use KafkaEx.API" do
    test "metadata/1 with a list of topic names requests only those topics", %{fake: fake} do
      booted = count(fake, :metadata)

      assert {:ok, _} = Bound.metadata(["a", "b"])

      assert requested_topics(fake, booted) == [["a", "b"]]
    end

    test "metadata/2 mirrors metadata/3 with topics and options", %{fake: fake} do
      booted = count(fake, :metadata)

      assert {:ok, _} = Bound.metadata(["a"], api_version: 2)

      assert requested_topics(fake, booted) == [["a"]]
    end

    test "metadata/1 with keyword options still requests all topics", %{fake: fake} do
      booted = count(fake, :metadata)

      assert {:ok, _} = Bound.metadata(api_version: 2)

      assert requested_topics(fake, booted) == [:all]
    end
  end
end
