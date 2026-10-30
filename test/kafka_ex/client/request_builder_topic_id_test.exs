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
  end
end
