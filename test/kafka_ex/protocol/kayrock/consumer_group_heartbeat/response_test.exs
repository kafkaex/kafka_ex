defmodule KafkaEx.Protocol.Kayrock.ConsumerGroupHeartbeat.ResponseTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Messages.ConsumerGroupHeartbeat, as: Result
  alias KafkaEx.Messages.TopicPartitions
  alias KafkaEx.Protocol.Kayrock.ConsumerGroupHeartbeat
  alias KafkaEx.Protocol.Kayrock.ConsumerGroupHeartbeat.ResponseHelpers
  alias KafkaEx.Protocol.KayrockProtocol

  @t1 <<1::128>>
  @t2 <<2::128>>

  defp response(version, fields) do
    module = Module.concat([Kayrock.ConsumerGroupHeartbeat, "V#{version}", Response])

    struct(
      module,
      Keyword.merge(
        [
          throttle_time_ms: 0,
          error_code: 0,
          error_message: nil,
          member_id: "m-1",
          member_epoch: 3,
          heartbeat_interval_ms: 5_000,
          assignment: nil
        ],
        fields
      )
    )
  end

  for version <- [0, 1] do
    describe "V#{version} Response" do
      @version version

      test "parses an unchanged (nil) assignment as nil" do
        assert {:ok, %Result{} = result} = ConsumerGroupHeartbeat.Response.parse_response(response(@version, []))
        assert result.member_id == "m-1"
        assert result.member_epoch == 3
        assert result.heartbeat_interval_ms == 5_000
        assert result.throttle_time_ms == 0
        assert result.assignment == nil
        assert Result.assigned_partition_count(result) == nil
      end

      test "parses an assignment by topic id" do
        assignment = %{
          topic_partitions: [%{topic_id: @t1, partitions: [0, 1]}, %{topic_id: @t2, partitions: [3]}],
          tagged_fields: []
        }

        assert {:ok, result} =
                 ConsumerGroupHeartbeat.Response.parse_response(response(@version, assignment: assignment))

        assert result.assignment == [
                 %TopicPartitions{topic_id: @t1, topic_name: nil, partitions: [0, 1]},
                 %TopicPartitions{topic_id: @t2, topic_name: nil, partitions: [3]}
               ]

        assert Result.assigned_partition_count(result) == 3
      end

      test "an empty assignment is [] (no partitions), not nil" do
        assignment = %{topic_partitions: [], tagged_fields: []}

        assert {:ok, %Result{assignment: []}} =
                 ConsumerGroupHeartbeat.Response.parse_response(response(@version, assignment: assignment))
      end

      test "maps KIP-848 error codes and keeps the error message" do
        for {code, atom} <- [
              {110, :fenced_member_epoch},
              {113, :stale_member_epoch},
              {25, :unknown_member_id},
              {111, :unreleased_instance_id},
              {112, :unsupported_assignor},
              {16, :not_coordinator},
              {15, :coordinator_not_available},
              {14, :coordinator_load_in_progress},
              {69, :group_id_not_found}
            ] do
          resp = response(@version, error_code: code, error_message: "because")
          assert {:error, error} = ConsumerGroupHeartbeat.Response.parse_response(resp)
          assert error.error == atom
          assert error.metadata == %{error_message: "because"}
        end
      end

      test "an error without a message has empty metadata" do
        assert {:error, %{error: :fenced_member_epoch, metadata: %{}}} =
                 ConsumerGroupHeartbeat.Response.parse_response(response(@version, error_code: 110))
      end
    end
  end

  describe "put_topic_names/2" do
    setup do
      assignment = %{topic_partitions: [%{topic_id: @t1, partitions: [0]}, %{topic_id: @t2, partitions: [1]}]}
      {:ok, result} = ResponseHelpers.parse_response(response(1, assignment: assignment))
      {:ok, result: result}
    end

    test "fills in known names and leaves unknown ones nil", %{result: result} do
      resolver = fn
        @t1 -> {:ok, "topic-1"}
        _ -> {:error, :no_such_topic}
      end

      assert {:ok, %Result{assignment: [first, second]}} = ResponseHelpers.put_topic_names({:ok, result}, resolver)
      assert first.topic_name == "topic-1"
      assert second.topic_name == nil
    end

    test "nil assignments and errors pass through" do
      resolver = fn _ -> flunk("must not be called") end
      {:ok, unchanged} = ResponseHelpers.parse_response(response(1, []))

      assert {:ok, ^unchanged} = ResponseHelpers.put_topic_names({:ok, unchanged}, resolver)
      assert {:error, :x} = ResponseHelpers.put_topic_names({:error, :x}, resolver)
    end

    test "KayrockProtocol.parse_response/3 applies the resolver" do
      assignment = %{topic_partitions: [%{topic_id: @t1, partitions: [0]}]}

      assert {:ok, %Result{assignment: [%TopicPartitions{topic_name: "topic-1"}]}} =
               KayrockProtocol.parse_response(:consumer_group_heartbeat, response(1, assignment: assignment),
                 topic_name_resolver: fn @t1 -> {:ok, "topic-1"} end
               )

      assert {:ok, %Result{assignment: [%TopicPartitions{topic_name: nil}]}} =
               KayrockProtocol.parse_response(:consumer_group_heartbeat, response(1, assignment: assignment), [])
    end
  end

  describe "Any fallback" do
    test "parses a plain map" do
      map = response(1, []) |> Map.from_struct()
      assert {:ok, %Result{member_epoch: 3}} = ConsumerGroupHeartbeat.Response.parse_response(map)
    end
  end

  describe "wire round trip" do
    test "deserializes a broker response with an assignment" do
      # throttle, error_code, error_message(null), member_id "m", epoch, interval,
      # assignment present (int8 1) with one topic (compact array len 1+1) and partitions [0, 1]
      body =
        <<0::32, 0::16, 0, 2, "m", 7::32, 3000::32, 1, 2>> <>
          @t1 <> <<3, 0::32, 1::32, 0, 0, 0>>

      request = %{Kayrock.ConsumerGroupHeartbeat.get_request_struct(1) | correlation_id: 9}
      {response, _} = Kayrock.Request.response_deserializer(request).(<<9::32, 0>> <> body)

      assert {:ok, %Result{member_id: "m", member_epoch: 7, heartbeat_interval_ms: 3000, assignment: [tp]}} =
               ConsumerGroupHeartbeat.Response.parse_response(response)

      assert tp.topic_id == @t1
      assert tp.partitions == [0, 1]
    end
  end
end
