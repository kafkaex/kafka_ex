defmodule KafkaEx.Protocol.Kayrock.SyncGroup.ResponseTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.SyncGroup
  alias KafkaEx.Messages.ConsumerGroupDescription.Member.MemberAssignment.PartitionAssignment

  # ---- Shared test helpers ----

  defp build_member_assignment(topics) do
    %Kayrock.MemberAssignment{
      version: 0,
      partition_assignments:
        Enum.map(topics, fn {topic, partitions} ->
          %Kayrock.MemberAssignment.PartitionAssignment{
            topic: topic,
            partitions: partitions
          }
        end),
      user_data: ""
    }
  end

  defp empty_member_assignment do
    build_member_assignment([])
  end

  # ---- V0 Response ----

  describe "V0 Response implementation" do
    test "parses successful response with no error and empty assignments" do
      response = %Kayrock.SyncGroup.V0.Response{
        error_code: 0,
        assignment: empty_member_assignment()
      }

      assert {:ok, result} = SyncGroup.Response.parse_response(response)
      assert result.throttle_time_ms == nil
      assert result.partition_assignments == []
    end

    test "parses successful response with partition assignments" do
      response = %Kayrock.SyncGroup.V0.Response{
        error_code: 0,
        assignment: build_member_assignment([{"test-topic", [0, 1, 2]}])
      }

      assert {:ok, result} = SyncGroup.Response.parse_response(response)
      assert result.throttle_time_ms == nil
      assert length(result.partition_assignments) == 1

      [assignment] = result.partition_assignments
      assert %PartitionAssignment{topic: "test-topic", partitions: [0, 1, 2]} = assignment
    end

    test "parses successful response with multiple topics" do
      response = %Kayrock.SyncGroup.V0.Response{
        error_code: 0,
        assignment: build_member_assignment([{"topic-1", [0, 1]}, {"topic-2", [3, 4, 5]}])
      }

      assert {:ok, result} = SyncGroup.Response.parse_response(response)
      assert length(result.partition_assignments) == 2
    end

    test "parses error response with unknown_member_id" do
      response = %Kayrock.SyncGroup.V0.Response{
        error_code: 25,
        assignment: empty_member_assignment()
      }

      assert {:error, error} = SyncGroup.Response.parse_response(response)
      assert error.error == :unknown_member_id
      assert error.metadata == %{}
    end

    test "parses error response with rebalance_in_progress" do
      response = %Kayrock.SyncGroup.V0.Response{
        error_code: 27,
        assignment: empty_member_assignment()
      }

      assert {:error, error} = SyncGroup.Response.parse_response(response)
      assert error.error == :rebalance_in_progress
    end

    test "parses error response with illegal_generation" do
      response = %Kayrock.SyncGroup.V0.Response{
        error_code: 22,
        assignment: empty_member_assignment()
      }

      assert {:error, error} = SyncGroup.Response.parse_response(response)
      assert error.error == :illegal_generation
    end

    test "parses error response with coordinator_not_available" do
      response = %Kayrock.SyncGroup.V0.Response{
        error_code: 15,
        assignment: empty_member_assignment()
      }

      assert {:error, error} = SyncGroup.Response.parse_response(response)
      assert error.error == :coordinator_not_available
    end

    test "parses error response with not_coordinator" do
      response = %Kayrock.SyncGroup.V0.Response{
        error_code: 16,
        assignment: empty_member_assignment()
      }

      assert {:error, error} = SyncGroup.Response.parse_response(response)
      assert error.error == :not_coordinator
    end

    test "parses error response with group_authorization_failed" do
      response = %Kayrock.SyncGroup.V0.Response{
        error_code: 30,
        assignment: empty_member_assignment()
      }

      assert {:error, error} = SyncGroup.Response.parse_response(response)
      assert error.error == :group_authorization_failed
    end

    test "error struct has empty metadata" do
      response = %Kayrock.SyncGroup.V0.Response{
        error_code: 27,
        assignment: empty_member_assignment()
      }

      {:error, error} = SyncGroup.Response.parse_response(response)

      assert error.metadata == %{}
    end

    test "handles non-MemberAssignment assignment (fallback to empty)" do
      response = %Kayrock.SyncGroup.V0.Response{
        error_code: 0,
        assignment: <<1, 2, 3>>
      }

      assert {:ok, result} = SyncGroup.Response.parse_response(response)
      assert result.partition_assignments == []
    end

    test "handles nil assignment (fallback to empty)" do
      response = %Kayrock.SyncGroup.V0.Response{
        error_code: 0,
        assignment: nil
      }

      assert {:ok, result} = SyncGroup.Response.parse_response(response)
      assert result.partition_assignments == []
    end
  end

  # ---- V1 Response ----

  describe "V1 Response implementation" do
    test "parses successful response with no error and empty assignments" do
      response = %Kayrock.SyncGroup.V1.Response{
        throttle_time_ms: 100,
        error_code: 0,
        assignment: empty_member_assignment()
      }

      assert {:ok, result} = SyncGroup.Response.parse_response(response)
      assert result.throttle_time_ms == 100
      assert result.partition_assignments == []
    end

    test "parses successful response with zero throttle time" do
      response = %Kayrock.SyncGroup.V1.Response{
        throttle_time_ms: 0,
        error_code: 0,
        assignment: empty_member_assignment()
      }

      assert {:ok, result} = SyncGroup.Response.parse_response(response)
      assert result.throttle_time_ms == 0
    end

    test "parses successful response with partition assignments" do
      response = %Kayrock.SyncGroup.V1.Response{
        throttle_time_ms: 50,
        error_code: 0,
        assignment: build_member_assignment([{"test-topic", [0, 1, 2]}])
      }

      assert {:ok, result} = SyncGroup.Response.parse_response(response)
      assert result.throttle_time_ms == 50
      assert length(result.partition_assignments) == 1

      [assignment] = result.partition_assignments
      assert %PartitionAssignment{topic: "test-topic", partitions: [0, 1, 2]} = assignment
    end

    test "parses successful response with multiple topics" do
      response = %Kayrock.SyncGroup.V1.Response{
        throttle_time_ms: 25,
        error_code: 0,
        assignment:
          build_member_assignment([
            {"topic-1", [0, 1]},
            {"topic-2", [3, 4, 5]},
            {"topic-3", [10]}
          ])
      }

      assert {:ok, result} = SyncGroup.Response.parse_response(response)
      assert result.throttle_time_ms == 25
      assert length(result.partition_assignments) == 3
    end

    test "parses error response with unknown_member_id" do
      response = %Kayrock.SyncGroup.V1.Response{
        throttle_time_ms: 10,
        error_code: 25,
        assignment: empty_member_assignment()
      }

      assert {:error, error} = SyncGroup.Response.parse_response(response)
      assert error.error == :unknown_member_id
      assert error.metadata == %{}
    end

    test "parses error response with rebalance_in_progress" do
      response = %Kayrock.SyncGroup.V1.Response{
        throttle_time_ms: 0,
        error_code: 27,
        assignment: empty_member_assignment()
      }

      assert {:error, error} = SyncGroup.Response.parse_response(response)
      assert error.error == :rebalance_in_progress
    end

    test "parses error response with illegal_generation" do
      response = %Kayrock.SyncGroup.V1.Response{
        throttle_time_ms: 5,
        error_code: 22,
        assignment: empty_member_assignment()
      }

      assert {:error, error} = SyncGroup.Response.parse_response(response)
      assert error.error == :illegal_generation
    end
  end

  # ---- V2 Response ----

  describe "V2 Response implementation" do
    test "parses successful response with throttle_time_ms" do
      response = %Kayrock.SyncGroup.V2.Response{
        throttle_time_ms: 200,
        error_code: 0,
        assignment: build_member_assignment([{"test-topic", [0, 1]}])
      }

      assert {:ok, result} = SyncGroup.Response.parse_response(response)
      assert result.throttle_time_ms == 200
      assert length(result.partition_assignments) == 1

      [assignment] = result.partition_assignments
      assert %PartitionAssignment{topic: "test-topic", partitions: [0, 1]} = assignment
    end

    test "parses successful response with zero throttle time" do
      response = %Kayrock.SyncGroup.V2.Response{
        throttle_time_ms: 0,
        error_code: 0,
        assignment: empty_member_assignment()
      }

      assert {:ok, result} = SyncGroup.Response.parse_response(response)
      assert result.throttle_time_ms == 0
      assert result.partition_assignments == []
    end

    test "parses error response with unknown_member_id" do
      response = %Kayrock.SyncGroup.V2.Response{
        throttle_time_ms: 10,
        error_code: 25,
        assignment: empty_member_assignment()
      }

      assert {:error, error} = SyncGroup.Response.parse_response(response)
      assert error.error == :unknown_member_id
    end

    test "parses error response with rebalance_in_progress" do
      response = %Kayrock.SyncGroup.V2.Response{
        throttle_time_ms: 0,
        error_code: 27,
        assignment: empty_member_assignment()
      }

      assert {:error, error} = SyncGroup.Response.parse_response(response)
      assert error.error == :rebalance_in_progress
    end

    test "handles non-MemberAssignment assignment (fallback to empty)" do
      response = %Kayrock.SyncGroup.V2.Response{
        throttle_time_ms: 0,
        error_code: 0,
        assignment: <<1, 2, 3>>
      }

      assert {:ok, result} = SyncGroup.Response.parse_response(response)
      assert result.partition_assignments == []
    end
  end

  # ---- V3 Response ----

  describe "V3 Response implementation" do
    test "parses successful response with throttle_time_ms" do
      response = %Kayrock.SyncGroup.V3.Response{
        throttle_time_ms: 150,
        error_code: 0,
        assignment: build_member_assignment([{"test-topic", [0, 1, 2]}])
      }

      assert {:ok, result} = SyncGroup.Response.parse_response(response)
      assert result.throttle_time_ms == 150
      assert length(result.partition_assignments) == 1

      [assignment] = result.partition_assignments
      assert %PartitionAssignment{topic: "test-topic", partitions: [0, 1, 2]} = assignment
    end

    test "parses error response" do
      response = %Kayrock.SyncGroup.V3.Response{
        throttle_time_ms: 0,
        error_code: 25,
        assignment: empty_member_assignment()
      }

      assert {:error, error} = SyncGroup.Response.parse_response(response)
      assert error.error == :unknown_member_id
    end

    test "parses successful response with multiple topics" do
      response = %Kayrock.SyncGroup.V3.Response{
        throttle_time_ms: 75,
        error_code: 0,
        assignment:
          build_member_assignment([
            {"topic-a", [0]},
            {"topic-b", [1, 2]}
          ])
      }

      assert {:ok, result} = SyncGroup.Response.parse_response(response)
      assert result.throttle_time_ms == 75
      assert length(result.partition_assignments) == 2
    end

    test "handles non-MemberAssignment assignment (fallback to empty)" do
      response = %Kayrock.SyncGroup.V3.Response{
        throttle_time_ms: 0,
        error_code: 0,
        assignment: <<1, 2, 3>>
      }

      assert {:ok, result} = SyncGroup.Response.parse_response(response)
      assert result.partition_assignments == []
    end

    test "handles nil assignment (fallback to empty)" do
      response = %Kayrock.SyncGroup.V3.Response{
        throttle_time_ms: 0,
        error_code: 0,
        assignment: nil
      }

      assert {:ok, result} = SyncGroup.Response.parse_response(response)
      assert result.partition_assignments == []
    end

    test "parses successful response with zero throttle time" do
      response = %Kayrock.SyncGroup.V3.Response{
        throttle_time_ms: 0,
        error_code: 0,
        assignment: empty_member_assignment()
      }

      assert {:ok, result} = SyncGroup.Response.parse_response(response)
      assert result.throttle_time_ms == 0
      assert result.partition_assignments == []
    end
  end

  # ---- V4 Response (FLEX) ----

  describe "V4 Response implementation (FLEX)" do
    test "parses successful response (compact encoding handled by Kayrock)" do
      response = %Kayrock.SyncGroup.V4.Response{
        throttle_time_ms: 300,
        error_code: 0,
        assignment: build_member_assignment([{"test-topic", [0, 1]}]),
        tagged_fields: []
      }

      assert {:ok, result} = SyncGroup.Response.parse_response(response)
      assert result.throttle_time_ms == 300
      assert length(result.partition_assignments) == 1

      [assignment] = result.partition_assignments
      assert %PartitionAssignment{topic: "test-topic", partitions: [0, 1]} = assignment
    end

    test "parses error response" do
      response = %Kayrock.SyncGroup.V4.Response{
        throttle_time_ms: 0,
        error_code: 25,
        assignment: empty_member_assignment(),
        tagged_fields: []
      }

      assert {:error, error} = SyncGroup.Response.parse_response(response)
      assert error.error == :unknown_member_id
    end

    test "handles raw binary assignment (compact_bytes may not deserialize to MemberAssignment)" do
      # V4 uses compact_bytes for assignment. If Kayrock doesn't auto-deserialize
      # to MemberAssignment, extract_partition_assignments falls back to empty list.
      response = %Kayrock.SyncGroup.V4.Response{
        throttle_time_ms: 0,
        error_code: 0,
        assignment: <<0, 0, 0, 0, 0, 0, 0, 0>>,
        tagged_fields: []
      }

      assert {:ok, result} = SyncGroup.Response.parse_response(response)
      assert result.partition_assignments == []
    end

    test "handles nil assignment" do
      response = %Kayrock.SyncGroup.V4.Response{
        throttle_time_ms: 0,
        error_code: 0,
        assignment: nil,
        tagged_fields: []
      }

      assert {:ok, result} = SyncGroup.Response.parse_response(response)
      assert result.partition_assignments == []
    end

    test "preserves throttle_time_ms with tagged_fields" do
      response = %Kayrock.SyncGroup.V4.Response{
        throttle_time_ms: 500,
        error_code: 0,
        assignment: empty_member_assignment(),
        tagged_fields: [{0, <<1, 2, 3>>}]
      }

      assert {:ok, result} = SyncGroup.Response.parse_response(response)
      assert result.throttle_time_ms == 500
      assert result.partition_assignments == []
    end
  end

  # ---- Cross-version consistency ----

  describe "Cross-version response consistency (V1-V4)" do
    @success_assignment %Kayrock.MemberAssignment{
      version: 0,
      partition_assignments: [
        %Kayrock.MemberAssignment.PartitionAssignment{
          topic: "test-topic",
          partitions: [0, 1, 2]
        }
      ],
      user_data: ""
    }

    @v1_v4_response_structs [
      {struct(Kayrock.SyncGroup.V1.Response, %{
         throttle_time_ms: 100,
         error_code: 0,
         assignment: @success_assignment
       }), "V1"},
      {struct(Kayrock.SyncGroup.V2.Response, %{
         throttle_time_ms: 100,
         error_code: 0,
         assignment: @success_assignment
       }), "V2"},
      {struct(Kayrock.SyncGroup.V3.Response, %{
         throttle_time_ms: 100,
         error_code: 0,
         assignment: @success_assignment
       }), "V3"},
      {struct(Kayrock.SyncGroup.V4.Response, %{
         throttle_time_ms: 100,
         error_code: 0,
         assignment: @success_assignment,
         tagged_fields: []
       }), "V4"}
    ]

    test "V1-V4 produce identical domain results" do
      results =
        Enum.map(@v1_v4_response_structs, fn {response, label} ->
          {:ok, result} = SyncGroup.Response.parse_response(response)
          {label, result}
        end)

      for {label, result} <- results do
        assert result.throttle_time_ms == 100,
               "#{label}: expected throttle_time_ms 100, got #{result.throttle_time_ms}"

        assert length(result.partition_assignments) == 1,
               "#{label}: expected 1 partition assignment"

        [assignment] = result.partition_assignments
        assert assignment.topic == "test-topic"
        assert assignment.partitions == [0, 1, 2]
      end
    end

    test "V0 has nil throttle_time_ms while V1-V4 have non-nil" do
      v0_response = %Kayrock.SyncGroup.V0.Response{
        error_code: 0,
        assignment: @success_assignment
      }

      v1_response = %Kayrock.SyncGroup.V1.Response{
        throttle_time_ms: 0,
        error_code: 0,
        assignment: @success_assignment
      }

      {:ok, v0_result} = SyncGroup.Response.parse_response(v0_response)
      {:ok, v1_result} = SyncGroup.Response.parse_response(v1_response)

      assert v0_result.throttle_time_ms == nil
      assert v1_result.throttle_time_ms == 0

      # But partition assignments should be identical
      assert v0_result.partition_assignments == v1_result.partition_assignments
    end
  end

  # ---- Any fallback ----

  describe "Any fallback Response implementation" do
    test "routes V1+-like map to throttle_time path" do
      response = %{
        throttle_time_ms: 250,
        error_code: 0,
        assignment: %Kayrock.MemberAssignment{
          version: 0,
          partition_assignments: [
            %Kayrock.MemberAssignment.PartitionAssignment{
              topic: "any-topic",
              partitions: [0]
            }
          ],
          user_data: ""
        }
      }

      assert {:ok, result} = SyncGroup.Response.parse_response(response)
      assert result.throttle_time_ms == 250
      assert length(result.partition_assignments) == 1

      [assignment] = result.partition_assignments
      assert assignment.topic == "any-topic"
    end

    test "routes V0-like map to nil throttle_time path" do
      response = %{
        error_code: 0,
        assignment: %Kayrock.MemberAssignment{
          version: 0,
          partition_assignments: [],
          user_data: ""
        }
      }

      assert {:ok, result} = SyncGroup.Response.parse_response(response)
      assert result.throttle_time_ms == nil
      assert result.partition_assignments == []
    end

    test "handles error response via Any path" do
      response = %{
        throttle_time_ms: 0,
        error_code: 22,
        assignment: %Kayrock.MemberAssignment{
          version: 0,
          partition_assignments: [],
          user_data: ""
        }
      }

      assert {:error, error} = SyncGroup.Response.parse_response(response)
      assert error.error == :illegal_generation
    end

    test "handles V0-like error response via Any path" do
      response = %{
        error_code: 27,
        assignment: %Kayrock.MemberAssignment{
          version: 0,
          partition_assignments: [],
          user_data: ""
        }
      }

      assert {:error, error} = SyncGroup.Response.parse_response(response)
      assert error.error == :rebalance_in_progress
    end

    test "handles raw binary assignment via Any path (fallback to empty)" do
      response = %{
        throttle_time_ms: 0,
        error_code: 0,
        assignment: <<0, 0, 0, 0>>
      }

      assert {:ok, result} = SyncGroup.Response.parse_response(response)
      assert result.partition_assignments == []
    end

    test "handles nil assignment via Any V1+ path (fallback to empty)" do
      response = %{
        throttle_time_ms: 0,
        error_code: 0,
        assignment: nil
      }

      assert {:ok, result} = SyncGroup.Response.parse_response(response)
      assert result.throttle_time_ms == 0
      assert result.partition_assignments == []
    end

    test "handles nil assignment via Any V0 path (fallback to empty)" do
      response = %{
        error_code: 0,
        assignment: nil
      }

      assert {:ok, result} = SyncGroup.Response.parse_response(response)
      assert result.throttle_time_ms == nil
      assert result.partition_assignments == []
    end

    test "handles multiple topics via Any path" do
      response = %{
        throttle_time_ms: 50,
        error_code: 0,
        assignment: %Kayrock.MemberAssignment{
          version: 0,
          partition_assignments: [
            %Kayrock.MemberAssignment.PartitionAssignment{
              topic: "topic-1",
              partitions: [0, 1]
            },
            %Kayrock.MemberAssignment.PartitionAssignment{
              topic: "topic-2",
              partitions: [2]
            }
          ],
          user_data: ""
        }
      }

      assert {:ok, result} = SyncGroup.Response.parse_response(response)
      assert result.throttle_time_ms == 50
      assert length(result.partition_assignments) == 2
    end
  end
end
