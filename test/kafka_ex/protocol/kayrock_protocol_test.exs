defmodule KafkaEx.Protocol.KayrockProtocolTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.KayrockProtocol

  describe "build_request/3" do
    test "builds api_versions request" do
      request = KayrockProtocol.build_request(:api_versions, 0, [])

      assert request != nil
    end

    test "builds metadata request" do
      request = KayrockProtocol.build_request(:metadata, 0, topics: nil)

      assert request != nil
    end

    test "builds list_offsets request" do
      request =
        KayrockProtocol.build_request(:list_offsets, 0,
          topics: [{"test-topic", [%{partition_num: 0, timestamp: -1}]}],
          replica_id: -1
        )

      assert request != nil
    end

    test "builds offset_fetch request" do
      request =
        KayrockProtocol.build_request(:offset_fetch, 0,
          group_id: "test-group",
          topics: [{"test-topic", [%{partition_num: 0}]}]
        )

      assert request != nil
    end

    test "builds offset_commit request" do
      request =
        KayrockProtocol.build_request(:offset_commit, 0,
          group_id: "test-group",
          topics: [{"test-topic", [%{partition_num: 0, offset: 100}]}]
        )

      assert request != nil
    end

    test "builds heartbeat request" do
      request =
        KayrockProtocol.build_request(:heartbeat, 0,
          group_id: "test-group",
          generation_id: 1,
          member_id: "member-1"
        )

      assert request != nil
    end

    test "builds join_group request" do
      request =
        KayrockProtocol.build_request(:join_group, 0,
          group_id: "test-group",
          session_timeout: 30_000,
          member_id: "",
          protocol_type: "consumer",
          group_protocols: []
        )

      assert request != nil
    end

    test "builds leave_group request" do
      request =
        KayrockProtocol.build_request(:leave_group, 0,
          group_id: "test-group",
          member_id: "member-1"
        )

      assert request != nil
    end

    test "builds sync_group request" do
      request =
        KayrockProtocol.build_request(:sync_group, 0,
          group_id: "test-group",
          generation_id: 1,
          member_id: "member-1",
          group_assignment: []
        )

      assert request != nil
    end

    test "builds produce request" do
      request =
        KayrockProtocol.build_request(:produce, 0,
          topic: "test-topic",
          partition: 0,
          messages: [%{value: "hello"}]
        )

      assert request != nil
    end

    test "builds fetch request" do
      request =
        KayrockProtocol.build_request(:fetch, 0,
          topic: "test-topic",
          partition: 0,
          offset: 0,
          max_bytes: 1_000_000,
          replica_id: -1
        )

      assert request != nil
    end

    test "builds find_coordinator request" do
      request =
        KayrockProtocol.build_request(:find_coordinator, 0, group_id: "test-group")

      assert request != nil
    end

    test "builds create_topics request" do
      request =
        KayrockProtocol.build_request(:create_topics, 0,
          topics: [%{topic: "new-topic", num_partitions: 1, replication_factor: 1}],
          timeout: 30_000
        )

      assert request != nil
    end

    test "builds delete_topics request" do
      request =
        KayrockProtocol.build_request(:delete_topics, 0,
          topics: ["topic-to-delete"],
          timeout: 30_000
        )

      assert request != nil
    end

    test "builds describe_groups request" do
      request =
        KayrockProtocol.build_request(:describe_groups, 0, group_names: ["test-group"])

      assert request != nil
    end

    test "builds list_groups request" do
      request = KayrockProtocol.build_request(:list_groups, 0, [])
      assert %Kayrock.ListGroups.V0.Request{} = request
    end
  end

  describe "parse_response/2" do
    test "parses list_groups response" do
      response = %Kayrock.ListGroups.V0.Response{
        error_code: 0,
        groups: [%{group_id: "g1", protocol_type: "consumer"}]
      }

      assert {:ok, [%KafkaEx.Messages.ConsumerGroupListing{group_id: "g1", protocol_type: "consumer"}]} =
               KayrockProtocol.parse_response(:list_groups, response)
    end
  end

  describe "request_info/1" do
    test "extracts operation and version from metadata request" do
      request = %Kayrock.Metadata.V1.Request{topics: []}
      assert KayrockProtocol.request_info(request) == {:metadata, 1}
    end

    test "extracts operation and version from produce request" do
      request = %Kayrock.Produce.V2.Request{acks: 1, timeout: 5000, topic_data: []}
      assert KayrockProtocol.request_info(request) == {:produce, 2}
    end

    test "extracts operation and version from fetch request" do
      request = %Kayrock.Fetch.V3.Request{
        replica_id: -1,
        max_wait_time: 500,
        min_bytes: 1,
        max_bytes: 1_000_000,
        topics: []
      }

      assert KayrockProtocol.request_info(request) == {:fetch, 3}
    end

    test "extracts operation and version from api_versions request" do
      request = %Kayrock.ApiVersions.V0.Request{}
      assert KayrockProtocol.request_info(request) == {:api_versions, 0}
    end

    test "returns unknown for non-Kayrock struct" do
      request = %{__struct__: SomeUnknown.Module}
      assert KayrockProtocol.request_info(request) == {:unknown, 0}
    end
  end

  describe "max_supported_version/1" do
    # Every API KafkaEx builds requests for has a directory of explicit
    # vN_request_impl.ex / vN_response_impl.ex files; the cap must equal the
    # highest N present for both, so adding a version's impls without raising
    # the cap (or raising the cap without impls) fails here.
    @impl_root Path.expand("../../../lib/kafka_ex/protocol/kayrock", __DIR__)

    defp implemented_versions(api, kind) do
      Path.join([@impl_root, Atom.to_string(api), "v*_#{kind}_impl.ex"])
      |> Path.wildcard()
      |> Enum.map(fn path ->
        [_, n] = Regex.run(~r/v(\d+)_#{kind}_impl\.ex$/, path)
        String.to_integer(n)
      end)
    end

    test "the cap table covers exactly the APIs with impl directories" do
      dirs =
        @impl_root
        |> File.ls!()
        |> Enum.filter(&File.dir?(Path.join(@impl_root, &1)))
        |> Enum.map(&String.to_atom/1)
        |> Enum.sort()

      assert dirs == KayrockProtocol.implemented_max_versions() |> Map.keys() |> Enum.sort()
    end

    test "each cap is the highest version with both request and response impls" do
      for {api, cap} <- KayrockProtocol.implemented_max_versions() do
        requests = implemented_versions(api, "request")
        responses = implemented_versions(api, "response")

        assert Enum.max(requests) == cap, "#{api}: request impls go up to v#{Enum.max(requests)}, cap is v#{cap}"
        assert Enum.max(responses) == cap, "#{api}: response impls go up to v#{Enum.max(responses)}, cap is v#{cap}"
        assert Enum.sort(requests) == Enum.to_list(0..cap), "#{api}: request impls have gaps"
        assert Enum.sort(responses) == Enum.to_list(0..cap), "#{api}: response impls have gaps"
      end
    end

    test "never exceeds the cap, even when Kayrock generates newer versions" do
      for {api, cap} <- KayrockProtocol.implemented_max_versions() do
        {_, kayrock_max} = Kayrock.KafkaSchemaMetadata.version_range(api)
        assert KayrockProtocol.max_supported_version(api) == min(cap, kayrock_max)
      end
    end
  end
end
