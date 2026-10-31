defmodule KafkaEx.Protocol.Kayrock.OffsetCommit.Kip848VersionsTest do
  @moduledoc "OffsetCommit V9 (generation_id_or_member_epoch) and V10 (topic ids)."
  use ExUnit.Case, async: true

  alias KafkaEx.Client.Error
  alias KafkaEx.Messages.Offset
  alias KafkaEx.Messages.Offset.PartitionOffset
  alias KafkaEx.Protocol.Kayrock.OffsetCommit.Request, as: OffsetCommitRequest
  alias KafkaEx.Protocol.Kayrock.OffsetCommit.RequestHelpers
  alias KafkaEx.Protocol.Kayrock.OffsetCommit.Response, as: OffsetCommitResponse
  alias KafkaEx.Protocol.KayrockProtocol
  alias Kayrock.OffsetCommit.V9
  alias Kayrock.OffsetCommit.V10

  @id <<7::128>>

  defp opts(extra \\ []) do
    Keyword.merge(
      [
        group_id: "g",
        member_id: "m-1",
        generation_id: 5,
        topics: [{"t", [%{partition_num: 0, offset: 42, metadata: "md"}]}]
      ],
      extra
    )
  end

  defp serialize!(request) do
    request
    |> Map.merge(%{correlation_id: 1, client_id: "test"})
    |> Kayrock.Request.serialize()
    |> IO.iodata_to_binary()
  end

  describe "V9 request" do
    test "maps :generation_id to generation_id_or_member_epoch" do
      request = OffsetCommitRequest.build_request(%V9.Request{}, opts(group_instance_id: "static-1"))

      assert %V9.Request{
               group_id: "g",
               generation_id_or_member_epoch: 5,
               member_id: "m-1",
               group_instance_id: "static-1",
               topics: [
                 %{
                   name: "t",
                   partitions: [
                     %{
                       partition_index: 0,
                       committed_offset: 42,
                       committed_metadata: "md",
                       committed_leader_epoch: -1
                     }
                   ]
                 }
               ]
             } = request

      refute Map.has_key?(request, :generation_id)
      assert is_binary(serialize!(request))
    end

    test ":member_epoch takes precedence over :generation_id" do
      request = OffsetCommitRequest.build_request(%V9.Request{}, opts(member_epoch: 11))
      assert request.generation_id_or_member_epoch == 11
    end

    test "defaults to -1 and empty member id for standalone commits" do
      request =
        OffsetCommitRequest.build_request(%V9.Request{},
          group_id: "g",
          topics: [{"t", [%{partition_num: 0, offset: 1}]}]
        )

      assert request.generation_id_or_member_epoch == -1
      assert request.member_id == ""
      assert request.group_instance_id == nil
      assert is_binary(serialize!(request))
    end

    test "explicit generation_id: nil falls back to -1" do
      assert RequestHelpers.extract_generation_id_or_member_epoch(generation_id: nil) == -1
    end
  end

  describe "V10 request" do
    test "identifies topics by id" do
      request = OffsetCommitRequest.build_request(%V10.Request{}, opts(topic_ids: %{"t" => @id}))

      assert %V10.Request{
               generation_id_or_member_epoch: 5,
               topics: [%{topic_id: @id, partitions: [%{partition_index: 0, committed_offset: 42}]}]
             } = request

      refute Map.has_key?(hd(request.topics), :name)
      assert is_binary(serialize!(request))
    end

    test "raises when a topic id is missing" do
      assert_raise ArgumentError, ~r/no topic id known for topic "t"/, fn ->
        OffsetCommitRequest.build_request(%V10.Request{}, opts(topic_ids: %{}))
      end
    end
  end

  describe "V9 response" do
    test "parses successful commits" do
      response = %V9.Response{
        throttle_time_ms: 0,
        topics: [%{name: "t", partitions: [%{partition_index: 0, error_code: 0}]}]
      }

      assert {:ok, [%Offset{topic: "t", partition_offsets: [%PartitionOffset{partition: 0, error_code: :no_error}]}]} =
               OffsetCommitResponse.parse_response(response)
    end

    for {code, atom} <- [
          {113, :stale_member_epoch},
          {110, :fenced_member_epoch},
          {69, :group_id_not_found},
          {25, :unknown_member_id}
        ] do
      test "surfaces #{atom}" do
        response = %V9.Response{
          throttle_time_ms: 0,
          topics: [%{name: "t", partitions: [%{partition_index: 0, error_code: unquote(code)}]}]
        }

        assert {:error, %Error{error: unquote(atom), metadata: %{topic: "t", partition: 0}}} =
                 OffsetCommitResponse.parse_response(response)
      end
    end
  end

  describe "V10 response" do
    defp v10_response(error_code \\ 0) do
      %V10.Response{
        throttle_time_ms: 0,
        topics: [%{topic_id: @id, partitions: [%{partition_index: 3, error_code: error_code}]}]
      }
    end

    test "resolves topic names through the dispatch hub" do
      resolver = fn
        @id -> {:ok, "t"}
        _ -> {:error, :no_such_topic}
      end

      assert {:ok, [%Offset{topic: "t", partition_offsets: [%PartitionOffset{partition: 3}]}]} =
               KayrockProtocol.parse_response(:offset_commit, v10_response(), topic_name_resolver: resolver)
    end

    test "unknown topic id -> :unknown_topic_id" do
      unknown = fn _ -> {:error, :no_such_topic} end

      assert {:error, %Error{error: :unknown_topic_id, metadata: %{topic_id: @id}}} =
               KayrockProtocol.parse_response(:offset_commit, v10_response(), topic_name_resolver: unknown)

      assert {:error, %Error{error: :unknown_topic_id}} = OffsetCommitResponse.parse_response(v10_response())
    end

    test "no resolver -> :topic_name_resolver_required, not a broker-style :unknown_topic_id" do
      # The client refreshes metadata on :unknown_topic_id; a caller that
      # forgot the resolver must not look like a stale topic id.
      for result <- [
            KayrockProtocol.parse_response(:offset_commit, v10_response()),
            KayrockProtocol.parse_response(:offset_commit, v10_response(), []),
            KayrockProtocol.parse_response(:offset_commit, v10_response(), topic_name_resolver: nil)
          ] do
        assert {:error, %Error{error: :topic_name_resolver_required, metadata: %{topic_id: @id}}} = result
      end
    end

    test "partition errors still surface" do
      resolver = fn _ -> {:ok, "t"} end

      assert {:error, %Error{error: :stale_member_epoch, metadata: %{topic: "t", partition: 3}}} =
               KayrockProtocol.parse_response(:offset_commit, v10_response(113), topic_name_resolver: resolver)
    end
  end

  describe "via KayrockProtocol" do
    test "build_request dispatches V9 and V10" do
      assert %V9.Request{} = KayrockProtocol.build_request(:offset_commit, 9, opts())
      assert %V10.Request{} = KayrockProtocol.build_request(:offset_commit, 10, opts(topic_ids: %{"t" => @id}))
    end

    test "parse_response/3 is a no-op resolver for name-based versions" do
      response = %Kayrock.OffsetCommit.V8.Response{
        throttle_time_ms: 0,
        topics: [%{name: "t", partitions: [%{partition_index: 0, error_code: 0}]}]
      }

      assert KayrockProtocol.parse_response(:offset_commit, response, []) ==
               KayrockProtocol.parse_response(:offset_commit, response)
    end
  end

  describe "Any fallback" do
    defmodule FakeV11Request do
      defstruct [:group_id, :generation_id_or_member_epoch, :member_id, :group_instance_id, :topics]
    end

    test "uses the V9 builder for structs with generation_id_or_member_epoch" do
      request = OffsetCommitRequest.build_request(%FakeV11Request{}, opts(member_epoch: 2))
      assert request.generation_id_or_member_epoch == 2
      assert [%{name: "t"}] = request.topics
    end
  end
end
