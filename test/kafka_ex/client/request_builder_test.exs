defmodule KafkaEx.Client.RequestBuilderTest do
  # Logger.error from RequestBuilder + capture_log and Application.put_env: keep sync.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias KafkaEx.Client.RequestBuilder
  alias KafkaEx.Client.State
  alias KafkaEx.Test.KayrockFixtures, as: Fixtures

  @fetch [topic: "t", partition: 0, offset: 0]

  defp with_api_versions(versions) do
    Application.put_env(:kafka_ex, :api_versions, versions)
    on_exit(fn -> Application.delete_env(:kafka_ex, :api_versions) end)
  end

  describe "build/3 version selection" do
    test "uses the negotiated max when neither the call nor the app config picks one" do
      assert {:ok, request} = RequestBuilder.build(:fetch, @fetch, %State{api_versions: %{1 => {0, 8}}})
      assert Fixtures.request_type?(request, :fetch, 8)
    end

    test "caps the negotiated max at the highest version KafkaEx builds" do
      assert {:ok, request} = RequestBuilder.build(:fetch, @fetch, %State{api_versions: %{1 => {0, 99}}})
      assert Fixtures.request_type?(request, :fetch, 11)
    end

    test "the app config overrides the negotiated max, for its API only" do
      with_api_versions(%{fetch: 5})
      state = %State{api_versions: %{0 => {0, 6}, 1 => {0, 8}}}

      assert {:ok, fetch} = RequestBuilder.build(:fetch, @fetch, state)

      assert {:ok, produce} =
               RequestBuilder.build(:produce, [topic: "t", partition: 0, messages: [%{value: "v"}]], state)

      assert Fixtures.request_type?(fetch, :fetch, 5)
      assert Fixtures.request_type?(produce, :produce, 6)
    end

    test "an app config version 0 is respected" do
      with_api_versions(%{fetch: 0})
      assert {:ok, request} = RequestBuilder.build(:fetch, @fetch, %State{api_versions: %{1 => {0, 8}}})
      assert Fixtures.request_type?(request, :fetch, 0)
    end

    test "an empty app config falls through to the negotiated max" do
      with_api_versions(%{})
      assert {:ok, request} = RequestBuilder.build(:fetch, @fetch, %State{api_versions: %{1 => {0, 8}}})
      assert Fixtures.request_type?(request, :fetch, 8)
    end

    test "a per-call api_version overrides the app config" do
      with_api_versions(%{fetch: 5})
      opts = [{:api_version, 3} | @fetch]
      assert {:ok, request} = RequestBuilder.build(:fetch, opts, %State{api_versions: %{1 => {0, 8}}})
      assert Fixtures.request_type?(request, :fetch, 3)
    end

    test "a per-call api_version above the connection's max is refused, never downgraded" do
      state = %State{api_versions: %{1 => {0, 5}}}

      log =
        capture_log(fn ->
          assert {:error, :api_version_no_supported} = RequestBuilder.build(:fetch, [{:api_version, 8} | @fetch], state)
        end)

      assert log =~ "Requested API version 8 for fetch exceeds max supported version 5."
    end

    test "an app config version above the connection's max is refused" do
      with_api_versions(%{fetch: 15})

      {result, _log} = with_log(fn -> RequestBuilder.build(:fetch, @fetch, %State{api_versions: %{1 => {0, 8}}}) end)

      assert result == {:error, :api_version_no_supported}
    end

    test "an API the broker does not advertise is refused unless the call names a version" do
      state = %State{api_versions: %{}}

      log =
        capture_log(fn ->
          assert {:error, :api_not_supported_by_broker} = RequestBuilder.build(:fetch, @fetch, state)
        end)

      assert log =~ "Kafka API fetch is not supported by the connected broker."
      assert {:ok, request} = RequestBuilder.build(:fetch, [{:api_version, 10} | @fetch], state)
      assert Fixtures.request_type?(request, :fetch, 10)
    end

    test "ApiVersions is built at v0 unless the call names a version" do
      state = %State{api_versions: %{18 => {0, 3}}}

      assert capture_log(fn -> assert {:ok, _} = RequestBuilder.build(:api_versions, [], state) end) == ""
      assert {:ok, v0} = RequestBuilder.build(:api_versions, [], %State{api_versions: %{}})
      assert {:ok, v1} = RequestBuilder.build(:api_versions, [api_version: 1], state)
      assert Fixtures.request_type?(v0, :api_versions, 0)
      assert Fixtures.request_type?(v1, :api_versions, 1)
    end
  end

  describe "build/3 options" do
    test "an option the API does not send is dropped, not passed to the protocol" do
      state = %State{api_versions: %{2 => {0, 5}}}
      opts = [topics: [{"t", [%{partition_num: 0, timestamp: -1}]}], isolation_level: 1, replica_id: 7]

      assert {:ok, request} = RequestBuilder.build(:list_offsets, opts, state)
      assert %{isolation_level: 0, replica_id: -1} = request
    end

    test "the optional fields an API sends reach its request" do
      state = %State{api_versions: %{9 => {0, 7}, 12 => {0, 3}, 14 => {0, 3}, 69 => {0, 0}}}
      member = [group_id: "g", member_id: "m", generation_id: 1, group_instance_id: "i"]

      assert {:ok, heartbeat} = RequestBuilder.build(:heartbeat, member, state)
      assert {:ok, sync} = RequestBuilder.build(:sync_group, member, state)

      assert {:ok, fetch} =
               RequestBuilder.build(:offset_fetch, [group_id: "g", topics: [], require_stable: true], state)

      assert {:ok, describe} =
               RequestBuilder.build(
                 :consumer_group_describe,
                 [group_ids: ["g"], include_authorized_operations: true],
                 state
               )

      assert %{group_instance_id: "i"} = heartbeat
      assert %{group_instance_id: "i"} = sync
      assert %{require_stable: true} = fetch
      assert %{include_authorized_operations: true} = describe
    end

    test "a fetch carries the selected version into its topic layout" do
      assert {:ok, request} = RequestBuilder.build(:fetch, @fetch, %State{api_versions: %{1 => {0, 5}}})
      assert [%{partitions: [%{log_start_offset: 0}]}] = request.topics
    end

    test "LeaveGroup v3+ sends a members array, built from member_id unless members are given" do
      state = %State{api_versions: %{13 => {0, 3}}}
      members = [%{member_id: "c-1"}, %{member_id: "c-2", group_instance_id: "static-1"}]

      assert {:ok, single} = RequestBuilder.build(:leave_group, [group_id: "g", member_id: "c-1"], state)

      assert {:ok, batch} =
               RequestBuilder.build(:leave_group, [group_id: "g", member_id: "c-1", members: members], state)

      assert {:ok, v0} = RequestBuilder.build(:leave_group, [group_id: "g", member_id: "c-1", api_version: 0], state)
      assert [%{member_id: "c-1"}] = single.members
      assert [%{member_id: "c-1"}, %{member_id: "c-2", group_instance_id: "static-1"}] = batch.members
      assert %{group_id: "g", member_id: "c-1"} = v0
    end
  end
end
