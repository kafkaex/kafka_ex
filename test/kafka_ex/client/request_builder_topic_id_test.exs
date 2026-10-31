defmodule KafkaEx.Client.RequestBuilderTopicIdTest do
  @moduledoc "RequestBuilder topic-id resolution for OffsetCommit/OffsetFetch v10 and Metadata v12+."
  # Logger.error from RequestBuilder + capture_log: keep sync (see request_builder_test.exs).
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias KafkaEx.Client.RequestBuilder
  alias KafkaEx.Client.State
  alias KafkaEx.Cluster.ClusterMetadata
  alias KafkaEx.Cluster.Topic

  @id <<42::128>>
  @topics [{"t", [%{partition_num: 0, offset: 5}]}]

  defp state(api_versions, topics \\ %{"t" => %Topic{name: "t", topic_id: @id}}) do
    %State{api_versions: api_versions, cluster_metadata: %ClusterMetadata{topics: topics}}
  end

  describe "offset_commit_request/2" do
    test "uses v10 with the topic id when it is known" do
      {:ok, request} =
        RequestBuilder.offset_commit_request([group_id: "g", generation_id: 3, topics: @topics], state(%{8 => {2, 10}}))

      assert %Kayrock.OffsetCommit.V10.Request{generation_id_or_member_epoch: 3, topics: [%{topic_id: @id}]} =
               request
    end

    test "falls back to v9 (by name) when the topic id is unknown" do
      {:ok, request} =
        RequestBuilder.offset_commit_request([group_id: "g", topics: @topics], state(%{8 => {2, 10}}, %{}))

      assert %Kayrock.OffsetCommit.V9.Request{topics: [%{name: "t"}]} = request
    end

    test "a pinned api_version: 10 without an id also falls back to v9" do
      # A per-call pin is how a GenConsumer `api_versions: %{offset_commit: 10}`
      # reaches here, so it must behave like the app-config pin.
      {:ok, request} =
        RequestBuilder.offset_commit_request(
          [group_id: "g", topics: @topics, api_version: 10],
          state(%{8 => {2, 10}}, %{})
        )

      assert %Kayrock.OffsetCommit.V9.Request{topics: [%{name: "t"}]} = request
    end

    test "an app-config pin of v10 without an id falls back to v9 the same way" do
      Application.put_env(:kafka_ex, :api_versions, %{offset_commit: 10})
      on_exit(fn -> Application.delete_env(:kafka_ex, :api_versions) end)

      {:ok, request} =
        RequestBuilder.offset_commit_request([group_id: "g", topics: @topics], state(%{8 => {2, 10}}, %{}))

      assert %Kayrock.OffsetCommit.V9.Request{} = request
    end

    test "Kafka 3.x broker (max v8) is unaffected" do
      {:ok, request} =
        RequestBuilder.offset_commit_request([group_id: "g", generation_id: 1, topics: @topics], state(%{8 => {0, 8}}))

      assert %Kayrock.OffsetCommit.V8.Request{generation_id: 1, topics: [%{name: "t"}]} = request
    end
  end

  describe "offset_fetch_request/2" do
    test "uses v10 with the topic id when it is known" do
      {:ok, request} = RequestBuilder.offset_fetch_request([group_id: "g", topics: @topics], state(%{9 => {1, 10}}))
      assert %Kayrock.OffsetFetch.V10.Request{groups: [%{group_id: "g", topics: [%{topic_id: @id}]}]} = request
    end

    test "falls back to v9 when the id is unknown" do
      {:ok, request} =
        RequestBuilder.offset_fetch_request([group_id: "g", topics: @topics], state(%{9 => {1, 10}}, %{}))

      assert %Kayrock.OffsetFetch.V9.Request{groups: [%{topics: [%{name: "t"}]}]} = request
    end

    test "a pinned api_version: 10 without an id also falls back to v9" do
      {:ok, request} =
        RequestBuilder.offset_fetch_request(
          [group_id: "g", topics: @topics, api_version: 10],
          state(%{9 => {1, 10}}, %{})
        )

      assert %Kayrock.OffsetFetch.V9.Request{groups: [%{topics: [%{name: "t"}]}]} = request
    end

    test "passes member_epoch / require_stable through" do
      {:ok, request} =
        RequestBuilder.offset_fetch_request(
          [group_id: "g", topics: @topics, member_id: "m", member_epoch: 4, require_stable: true, api_version: 9],
          state(%{9 => {1, 10}})
        )

      assert %Kayrock.OffsetFetch.V9.Request{groups: [%{member_id: "m", member_epoch: 4}], require_stable: true} =
               request
    end
  end

  describe "metadata_request/2" do
    test "sends topic-id lookups at v12+" do
      {:ok, request} = RequestBuilder.metadata_request([topics: nil, topic_ids: [@id]], state(%{3 => {0, 13}}))
      assert %Kayrock.Metadata.V13.Request{topics: [%{topic_id: @id, name: nil}]} = request
    end

    test "rejects topic-id lookups below v12" do
      capture_log(fn ->
        assert {:error, :api_version_no_supported} =
                 RequestBuilder.metadata_request([topics: nil, topic_ids: [@id]], state(%{3 => {0, 9}}))
      end)
    end

    test "named topics at v13 carry the null id" do
      {:ok, request} = RequestBuilder.metadata_request([topics: ["t"]], state(%{3 => {0, 13}}))
      assert %Kayrock.Metadata.V13.Request{topics: [%{name: "t", topic_id: nil}]} = request
    end

    test "malformed topic ids are an error, not a crash in the client" do
      # Built inside the client GenServer: these used to raise FunctionClauseError there.
      for bad <- [["6ba7b810-9dad-11d1-80b4-00c04fd430c8"], [nil], [<<1::64>>], "not-an-id", [:a]] do
        assert {:error, :invalid_topic_ids} =
                 RequestBuilder.metadata_request([topics: nil, topic_ids: bad], state(%{3 => {0, 13}})),
               "expected #{inspect(bad)} to be rejected"
      end
    end

    test "a bare 16-byte id is accepted (it is wrapped into a list)" do
      {:ok, request} = RequestBuilder.metadata_request([topics: nil, topic_ids: @id], state(%{3 => {0, 13}}))
      assert %Kayrock.Metadata.V13.Request{topics: [%{topic_id: @id, name: nil}]} = request
    end

    test "a mixed by-name and by-id lookup is rejected" do
      # Kafka ignores the names in a request that carries any topic id, so the
      # named topics would silently be missing from an {:ok, ...} result.
      assert {:error, :mixed_topic_names_and_ids} =
               RequestBuilder.metadata_request([topics: ["a"], topic_ids: [@id]], state(%{3 => {0, 13}}))
    end
  end

  describe "member option validation (commit/fetch)" do
    test "a non-integer or out-of-int32 member_epoch is an error, not a crash" do
      for bad <- ["3", 1.5, 4_294_967_296, -2_147_483_649] do
        assert {:error, :invalid_member_epoch} =
                 RequestBuilder.offset_commit_request(
                   [group_id: "g", topics: @topics, member_epoch: bad],
                   state(%{8 => {2, 10}})
                 ),
               "commit: expected #{inspect(bad)} to be rejected"

        assert {:error, :invalid_member_epoch} =
                 RequestBuilder.offset_fetch_request(
                   [group_id: "g", topics: @topics, member_epoch: bad],
                   state(%{9 => {1, 10}})
                 ),
               "fetch: expected #{inspect(bad)} to be rejected"
      end
    end

    test "a non-binary member_id is an error, not a crash" do
      assert {:error, :invalid_member_id} =
               RequestBuilder.offset_commit_request(
                 [group_id: "g", topics: @topics, member_id: 7],
                 state(%{8 => {2, 10}})
               )

      assert {:error, :invalid_member_id} =
               RequestBuilder.offset_fetch_request(
                 [group_id: "g", topics: @topics, member_id: 7, member_epoch: 1],
                 state(%{9 => {1, 10}})
               )
    end

    test "nil member_id / generation_id commit as \"not given\" on every version" do
      # A GenConsumer started without a consumer group (the documented
      # GenConsumer.start_link(Mod, group, topic, 0)) commits with
      # member_id: nil, generation_id: nil. OffsetCommit's member_id is a
      # non-nullable string, so these used to raise inside the client.
      for version <- 0..10, opts <- [[member_id: nil], [generation_id: nil], [member_id: nil, generation_id: nil]] do
        assert {:ok, request} =
                 RequestBuilder.offset_commit_request(
                   [group_id: "g", topics: @topics, api_version: version] ++ opts,
                   state(%{8 => {0, 10}})
                 )

        assert IO.iodata_to_binary(Kayrock.Request.serialize(%{request | correlation_id: 1, client_id: "c"})) != "",
               "v#{version} #{inspect(opts)} must serialize"

        if version >= 1 do
          assert Map.get(request, :member_id) == ""
          assert Map.get(request, :generation_id, Map.get(request, :generation_id_or_member_epoch)) == -1
        end
      end
    end

    test "a non-integer generation_id is an error, not a crash" do
      for bad <- ["1", 1.0, 4_294_967_296] do
        assert {:error, :invalid_generation_id} =
                 RequestBuilder.offset_commit_request(
                   [group_id: "g", topics: @topics, generation_id: bad],
                   state(%{8 => {2, 10}})
                 )
      end
    end

    test "valid member options at the int32 bounds still build" do
      for epoch <- [-2_147_483_648, 2_147_483_647, 0, -1] do
        assert {:ok, %Kayrock.OffsetCommit.V10.Request{generation_id_or_member_epoch: ^epoch}} =
                 RequestBuilder.offset_commit_request(
                   [group_id: "g", topics: @topics, member_id: "m", member_epoch: epoch],
                   state(%{8 => {2, 10}})
                 )
      end
    end
  end
end
