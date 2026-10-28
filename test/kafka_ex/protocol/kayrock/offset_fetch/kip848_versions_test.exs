defmodule KafkaEx.Protocol.Kayrock.OffsetFetch.Kip848VersionsTest do
  @moduledoc "OffsetFetch V7 (require_stable), V8 (groups), V9 (member epoch), V10 (topic ids)."
  use ExUnit.Case, async: true

  alias KafkaEx.Client.Error
  alias KafkaEx.Messages.Offset
  alias KafkaEx.Messages.Offset.PartitionOffset
  alias KafkaEx.Protocol.Kayrock.OffsetFetch.Request, as: OffsetFetchRequest
  alias KafkaEx.Protocol.Kayrock.OffsetFetch.Response, as: OffsetFetchResponse
  alias KafkaEx.Protocol.KayrockProtocol
  alias Kayrock.OffsetFetch.V7
  alias Kayrock.OffsetFetch.V8
  alias Kayrock.OffsetFetch.V9
  alias Kayrock.OffsetFetch.V10

  @id <<5::128>>

  defp opts(extra \\ []) do
    Keyword.merge([group_id: "g", topics: [{"t", [%{partition_num: 0}, %{partition_num: 1}]}]], extra)
  end

  defp serialize!(request) do
    request
    |> Map.merge(%{correlation_id: 1, client_id: "test"})
    |> Kayrock.Request.serialize()
    |> IO.iodata_to_binary()
  end

  defp partition(index, offset, error_code \\ 0) do
    %{
      partition_index: index,
      committed_offset: offset,
      committed_leader_epoch: 4,
      metadata: "md",
      error_code: error_code,
      tagged_fields: []
    }
  end

  describe "requests" do
    test "V7 adds require_stable (default false)" do
      request = OffsetFetchRequest.build_request(%V7.Request{}, opts())

      assert %V7.Request{
               group_id: "g",
               topics: [%{name: "t", partition_indexes: [0, 1]}],
               require_stable: false
             } = request

      assert OffsetFetchRequest.build_request(%V7.Request{}, opts(require_stable: true)).require_stable
      assert is_binary(serialize!(request))
    end

    test "V8 puts the group in groups[]" do
      request = OffsetFetchRequest.build_request(%V8.Request{}, opts())

      assert %V8.Request{
               groups: [%{group_id: "g", topics: [%{name: "t", partition_indexes: [0, 1]}]}],
               require_stable: false
             } = request

      assert is_binary(serialize!(request))
    end

    test "V9 classic fetch sends member_id nil / member_epoch -1" do
      request = OffsetFetchRequest.build_request(%V9.Request{}, opts(member_id: "classic-member"))
      assert [%{group_id: "g", member_id: nil, member_epoch: -1, topics: [%{name: "t"}]}] = request.groups
      assert is_binary(serialize!(request))
    end

    test "V9 with :member_epoch sends member identity" do
      request = OffsetFetchRequest.build_request(%V9.Request{}, opts(member_id: "m", member_epoch: 3))
      assert [%{member_id: "m", member_epoch: 3}] = request.groups
      assert is_binary(serialize!(request))
    end

    test "V10 identifies topics by id" do
      request = OffsetFetchRequest.build_request(%V10.Request{}, opts(topic_ids: %{"t" => @id}))
      assert [%{member_epoch: -1, topics: [%{topic_id: @id, partition_indexes: [0, 1]}]}] = request.groups
      assert is_binary(serialize!(request))
    end

    test "V10 raises when a topic id is missing" do
      assert_raise ArgumentError, ~r/no topic id known/, fn ->
        OffsetFetchRequest.build_request(%V10.Request{}, opts())
      end
    end
  end

  describe "V7 response" do
    test "parses like V6" do
      response = %V7.Response{
        throttle_time_ms: 0,
        topics: [%{name: "t", partitions: [partition(0, 10)], tagged_fields: []}],
        error_code: 0,
        tagged_fields: []
      }

      assert {:ok,
              [%Offset{topic: "t", partition_offsets: [%PartitionOffset{partition: 0, offset: 10, leader_epoch: 4}]}]} =
               OffsetFetchResponse.parse_response(response)
    end

    test "top-level error" do
      response = %V7.Response{throttle_time_ms: 0, topics: [], error_code: 16, tagged_fields: []}
      assert {:error, %Error{error: :not_coordinator}} = OffsetFetchResponse.parse_response(response)
    end
  end

  describe "V8/V9 responses" do
    for mod <- [V8, V9] do
      test "#{inspect(mod)} parses groups[] into the same Offset list" do
        response =
          struct(unquote(mod).Response, %{
            throttle_time_ms: 0,
            groups: [
              %{
                group_id: "g",
                topics: [%{name: "t", partitions: [partition(0, 10), partition(1, -1)], tagged_fields: []}],
                error_code: 0,
                tagged_fields: []
              }
            ],
            tagged_fields: []
          })

        assert {:ok, offsets} = OffsetFetchResponse.parse_response(response)

        assert offsets |> Enum.flat_map(& &1.partition_offsets) |> Enum.map(&{&1.partition, &1.offset}) |> Enum.sort() ==
                 [{0, 10}, {1, -1}]

        assert Enum.all?(offsets, &(&1.topic == "t"))
      end

      for {code, atom} <- [
            {16, :not_coordinator},
            {113, :stale_member_epoch},
            {25, :unknown_member_id},
            {110, :fenced_member_epoch}
          ] do
        test "#{inspect(mod)} group-level #{atom}" do
          response =
            struct(unquote(mod).Response, %{
              throttle_time_ms: 0,
              groups: [%{group_id: "g", topics: [], error_code: unquote(code), tagged_fields: []}],
              tagged_fields: []
            })

          assert {:error, %Error{error: unquote(atom)}} = OffsetFetchResponse.parse_response(response)
        end
      end

      test "#{inspect(mod)} partition-level error" do
        response =
          struct(unquote(mod).Response, %{
            throttle_time_ms: 0,
            groups: [
              %{group_id: "g", topics: [%{name: "t", partitions: [partition(0, -1, 3)]}], error_code: 0}
            ]
          })

        assert {:error, %Error{error: :unknown_topic_or_partition, metadata: %{topic: "t", partition: 0}}} =
                 OffsetFetchResponse.parse_response(response)
      end
    end

    test "empty groups[] is an error, not an empty success" do
      assert {:error, %Error{}} = OffsetFetchResponse.parse_response(%V8.Response{throttle_time_ms: 0, groups: []})
    end
  end

  describe "V10 response" do
    defp v10_response do
      %V10.Response{
        throttle_time_ms: 0,
        groups: [
          %{
            group_id: "g",
            topics: [%{topic_id: @id, partitions: [partition(0, 99)], tagged_fields: []}],
            error_code: 0,
            tagged_fields: []
          }
        ],
        tagged_fields: []
      }
    end

    test "resolves names through the dispatch hub" do
      resolver = fn
        @id -> {:ok, "t"}
        _ -> {:error, :no_such_topic}
      end

      assert {:ok, [%Offset{topic: "t", partition_offsets: [%PartitionOffset{offset: 99}]}]} =
               KayrockProtocol.parse_response(:offset_fetch, v10_response(), topic_name_resolver: resolver)
    end

    test "unknown topic id -> :unknown_topic_id" do
      assert {:error, %Error{error: :unknown_topic_id}} =
               KayrockProtocol.parse_response(:offset_fetch, v10_response(), [])
    end
  end

  describe "via KayrockProtocol" do
    for version <- 7..9 do
      test "build_request dispatches V#{version}" do
        request = KayrockProtocol.build_request(:offset_fetch, unquote(version), opts())
        assert request.__struct__ == Module.concat([Kayrock.OffsetFetch, :"V#{unquote(version)}", Request])
      end
    end

    test "build_request dispatches V10" do
      assert %V10.Request{} = KayrockProtocol.build_request(:offset_fetch, 10, opts(topic_ids: %{"t" => @id}))
    end
  end

  describe "Any fallback" do
    defmodule FakeGroupsRequest do
      defstruct [:groups, :require_stable]
    end

    defmodule FakeGroupsResponse do
      defstruct [:throttle_time_ms, :groups]
    end

    test "request with groups uses the batched shape" do
      request = OffsetFetchRequest.build_request(%FakeGroupsRequest{}, opts())
      assert [%{group_id: "g", member_epoch: -1, topics: [%{name: "t"}]}] = request.groups
      assert request.require_stable == false
    end

    test "response with groups parses the batched shape" do
      response = %FakeGroupsResponse{
        groups: [%{group_id: "g", topics: [%{name: "t", partitions: [partition(0, 1)]}], error_code: 0}]
      }

      assert {:ok, [%Offset{topic: "t"}]} = OffsetFetchResponse.parse_response(response)
    end
  end
end
