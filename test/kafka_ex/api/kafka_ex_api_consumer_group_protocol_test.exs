defmodule KafkaEx.APIConsumerGroupProtocolTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Client.RequestBuilder
  alias KafkaEx.Client.State
  alias KafkaEx.Messages.ConsumerGroupDescription
  alias KafkaEx.Messages.ConsumerGroupHeartbeat
  alias KafkaEx.Messages.TopicPartitions
  alias KafkaEx.Test.MockClient

  @heartbeat_api_key 68
  @describe_api_key 69

  defmodule Bound do
    @moduledoc false
    use KafkaEx.API

    def client, do: Process.get(:kafka_client)
  end

  describe "consumer_group_heartbeat/3" do
    test "passes the group and opts to the client and returns the result" do
      result = %ConsumerGroupHeartbeat{member_id: "m", member_epoch: 2, heartbeat_interval_ms: 5_000, assignment: nil}
      {:ok, client} = MockClient.start_link(%{consumer_group_heartbeat: {:ok, result}})

      assert {:ok, ^result} =
               KafkaEx.API.consumer_group_heartbeat(client, "g",
                 member_id: "m",
                 member_epoch: 1,
                 network_timeout: 3_000
               )

      assert [{:consumer_group_heartbeat, "g", opts}] = MockClient.get_calls(client)
      assert opts[:member_id] == "m"
      assert opts[:member_epoch] == 1
      assert opts[:network_timeout] == 3_000
    end

    test "defaults the per-attempt network timeout" do
      {:ok, client} = MockClient.start_link(%{})
      assert {:ok, _} = KafkaEx.API.consumer_group_heartbeat(client, "g", member_epoch: 0)
      assert [{:consumer_group_heartbeat, "g", opts}] = MockClient.get_calls(client)
      assert is_integer(opts[:network_timeout])
    end

    test "normalises client errors to atoms" do
      {:ok, client} =
        MockClient.start_link(%{consumer_group_heartbeat: {:error, %KafkaEx.Client.Error{error: :fenced_member_epoch}}})

      assert {:error, :fenced_member_epoch} = KafkaEx.API.consumer_group_heartbeat(client, "g", member_epoch: 5)
    end

    # Option validation runs in the real client (see
    # test/kafka_ex/client/consumer_group_protocol_test.exs); the API no
    # longer raises, so it keeps the {:ok, _} | {:error, _} contract.
    test "passes an invalid-option error from the client through as an atom" do
      {:ok, client} = MockClient.start_link(%{consumer_group_heartbeat: {:error, :invalid_member_epoch}})

      assert {:error, :invalid_member_epoch} = KafkaEx.API.consumer_group_heartbeat(client, "g", member_id: "m")
    end
  end

  describe "describe_consumer_group/3" do
    test "returns the single described group" do
      description = %ConsumerGroupDescription{group_id: "g", type: :consumer, members: []}
      {:ok, client} = MockClient.start_link(%{consumer_group_describe: {:ok, [description]}})

      assert {:ok, ^description} = KafkaEx.API.describe_consumer_group(client, "g", include_authorized_operations: true)
      assert [{:consumer_group_describe, ["g"], [include_authorized_operations: true]}] = MockClient.get_calls(client)
    end

    test "unwraps group-level errors" do
      {:ok, client} = MockClient.start_link(%{consumer_group_describe: {:error, {"g", :group_id_not_found}}})
      assert {:error, :group_id_not_found} = KafkaEx.API.describe_consumer_group(client, "g")
    end

    test "passes through client errors" do
      {:ok, client} = MockClient.start_link(%{consumer_group_describe: {:error, :api_not_supported_by_broker}})
      assert {:error, :api_not_supported_by_broker} = KafkaEx.API.describe_consumer_group(client, "g")

      {:ok, client} =
        MockClient.start_link(%{consumer_group_describe: {:error, %KafkaEx.Client.Error{error: :timeout}}})

      assert {:error, :timeout} = KafkaEx.API.describe_consumer_group(client, "g")
    end
  end

  describe "use KafkaEx.API" do
    test "generates consumer_group_heartbeat/2 and describe_consumer_group/1,2" do
      {:ok, client} =
        MockClient.start_link(%{consumer_group_describe: {:ok, [%ConsumerGroupDescription{group_id: "g"}]}})

      Process.put(:kafka_client, client)

      assert {:ok, %ConsumerGroupHeartbeat{}} = Bound.consumer_group_heartbeat("g", member_epoch: 0)
      assert {:ok, %ConsumerGroupDescription{group_id: "g"}} = Bound.describe_consumer_group("g")
      assert {:ok, %ConsumerGroupDescription{}} = Bound.describe_consumer_group("g", [])
    end
  end

  describe "RequestBuilder.consumer_group_heartbeat_request/2" do
    setup do
      {:ok, state: %State{api_versions: %{@heartbeat_api_key => {0, 1}}}}
    end

    test "builds the negotiated version with all heartbeat fields", %{state: state} do
      opts = [
        group_id: "g",
        member_id: "m",
        member_epoch: 0,
        instance_id: "i",
        rack_id: "r",
        rebalance_timeout_ms: 30_000,
        subscribed_topic_names: ["t"],
        subscribed_topic_regex: "t.*",
        server_assignor: "range",
        topic_partitions: [%TopicPartitions{topic_id: <<1::128>>, partitions: [0]}],
        network_timeout: 1
      ]

      assert {:ok, request} = RequestBuilder.consumer_group_heartbeat_request(opts, state)
      assert request.__struct__.api_vsn() == 1
      assert request.member_id == "m"
      assert request.instance_id == "i"
      assert request.rack_id == "r"
      assert request.rebalance_timeout_ms == 30_000
      assert request.subscribed_topic_names == ["t"]
      assert request.subscribed_topic_regex == "t.*"
      assert request.server_assignor == "range"
      assert request.topic_partitions == [%{topic_id: <<1::128>>, partitions: [0]}]
    end

    test "honours an explicit api_version", %{state: state} do
      assert {:ok, request} =
               RequestBuilder.consumer_group_heartbeat_request([group_id: "g", member_epoch: 0, api_version: 0], state)

      assert request.__struct__.api_vsn() == 0
      assert request.member_id == ""
    end

    test "rejects subscribed_topic_regex on v0", %{state: state} do
      opts = [group_id: "g", member_epoch: 0, subscribed_topic_regex: "t.*", api_version: 0]
      assert {:error, :api_version_no_supported} = RequestBuilder.consumer_group_heartbeat_request(opts, state)
    end

    test "errors when the broker lacks the API" do
      assert {:error, :api_not_supported_by_broker} =
               RequestBuilder.consumer_group_heartbeat_request([group_id: "g", member_epoch: 0], %State{
                 api_versions: %{}
               })
    end
  end

  describe "RequestBuilder.consumer_group_describe_request/2" do
    test "builds the negotiated version" do
      state = %State{api_versions: %{@describe_api_key => {0, 1}}}

      assert {:ok, request} =
               RequestBuilder.consumer_group_describe_request(
                 [group_ids: ["g"], include_authorized_operations: true],
                 state
               )

      assert request.__struct__.api_vsn() == 1
      assert request.group_ids == ["g"]
      assert request.include_authorized_operations == true
    end

    test "is capped at the implemented maximum even if the broker reports more" do
      state = %State{api_versions: %{@describe_api_key => {0, 9}}}
      assert {:ok, request} = RequestBuilder.consumer_group_describe_request([group_ids: ["g"]], state)
      assert request.__struct__.api_vsn() == 1
    end
  end

  describe "generate_member_id/0" do
    test "returns a KIP-1082 style id: 22 URL-safe base64 chars of a v4 UUID" do
      id = KafkaEx.API.generate_member_id()
      assert byte_size(id) == 22
      assert {:ok, <<_::48, 4::4, _::12, 2::2, _::62>>} = Base.url_decode64(id, padding: false)
    end

    test "is unique per call, so a caller must keep it across join retries" do
      ids = for _ <- 1..1000, do: KafkaEx.API.generate_member_id()
      assert length(Enum.uniq(ids)) == 1000
    end

    test "goes through the KayrockProtocol hub, not a per-operation protocol module" do
      # AGENTS.md: KafkaEx.Protocol.KayrockProtocol is the only module the rest
      # of the client talks to the protocol layer through.
      {:ok, {_, [imports: imports]}} = :beam_lib.chunks(:code.which(KafkaEx.API), [:imports])

      protocol_modules =
        for {module, _fun, _arity} <- imports,
            String.starts_with?(Atom.to_string(module), "Elixir.KafkaEx.Protocol.Kayrock."),
            uniq: true,
            do: module

      assert protocol_modules == []
      assert {KafkaEx.Protocol.KayrockProtocol, :generate_member_id, 0} in imports
    end
  end
end
