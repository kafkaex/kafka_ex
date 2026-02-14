defmodule KafkaEx.Protocol.Kayrock.SyncGroup.ResponseHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.SyncGroup.ResponseHelpers
  alias KafkaEx.Messages.SyncGroup
  alias KafkaEx.Messages.ConsumerGroupDescription.Member.MemberAssignment.PartitionAssignment
  alias KafkaEx.Client.Error

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

  # ---- extract_partition_assignments/1 ----

  describe "extract_partition_assignments/1" do
    test "extracts assignments from Kayrock.MemberAssignment struct with one topic" do
      member_assignment = build_member_assignment([{"test-topic", [0, 1, 2]}])

      result = ResponseHelpers.extract_partition_assignments(member_assignment)

      assert length(result) == 1
      assert [%PartitionAssignment{topic: "test-topic", partitions: [0, 1, 2]}] = result
    end

    test "extracts assignments from Kayrock.MemberAssignment struct with multiple topics" do
      member_assignment =
        build_member_assignment([
          {"topic-a", [0]},
          {"topic-b", [1, 2]},
          {"topic-c", [3, 4, 5]}
        ])

      result = ResponseHelpers.extract_partition_assignments(member_assignment)

      assert length(result) == 3
      assert Enum.at(result, 0).topic == "topic-a"
      assert Enum.at(result, 0).partitions == [0]
      assert Enum.at(result, 1).topic == "topic-b"
      assert Enum.at(result, 1).partitions == [1, 2]
      assert Enum.at(result, 2).topic == "topic-c"
      assert Enum.at(result, 2).partitions == [3, 4, 5]
    end

    test "returns empty list for empty Kayrock.MemberAssignment" do
      member_assignment = empty_member_assignment()

      result = ResponseHelpers.extract_partition_assignments(member_assignment)

      assert result == []
    end

    test "returns empty list for nil (catch-all fallback)" do
      assert ResponseHelpers.extract_partition_assignments(nil) == []
    end

    test "returns empty list for raw binary (catch-all fallback)" do
      assert ResponseHelpers.extract_partition_assignments(<<1, 2, 3, 4>>) == []
    end

    test "returns empty list for empty binary (catch-all fallback)" do
      assert ResponseHelpers.extract_partition_assignments(<<>>) == []
    end

    test "returns empty list for plain map (catch-all fallback)" do
      assert ResponseHelpers.extract_partition_assignments(%{some: "data"}) == []
    end

    test "returns empty list for atom (catch-all fallback)" do
      assert ResponseHelpers.extract_partition_assignments(:something) == []
    end

    test "returns empty list for integer (catch-all fallback)" do
      assert ResponseHelpers.extract_partition_assignments(42) == []
    end

    test "returns empty list for list (catch-all fallback)" do
      assert ResponseHelpers.extract_partition_assignments([1, 2, 3]) == []
    end

    test "preserves partition order from Kayrock struct" do
      member_assignment = build_member_assignment([{"topic", [5, 3, 1, 0]}])

      [assignment] = ResponseHelpers.extract_partition_assignments(member_assignment)

      assert assignment.partitions == [5, 3, 1, 0]
    end

    test "handles topic with empty partition list" do
      member_assignment = build_member_assignment([{"empty-topic", []}])

      [assignment] = ResponseHelpers.extract_partition_assignments(member_assignment)

      assert assignment.topic == "empty-topic"
      assert assignment.partitions == []
    end
  end

  # ---- parse_v0_response/1 ----

  describe "parse_v0_response/1" do
    test "parses successful V0 response with empty assignment" do
      response = %{
        error_code: 0,
        assignment: empty_member_assignment()
      }

      assert {:ok, %SyncGroup{} = result} = ResponseHelpers.parse_v0_response(response)
      assert result.throttle_time_ms == nil
      assert result.partition_assignments == []
    end

    test "parses successful V0 response with partition assignments" do
      response = %{
        error_code: 0,
        assignment: build_member_assignment([{"test-topic", [0, 1, 2]}])
      }

      assert {:ok, %SyncGroup{} = result} = ResponseHelpers.parse_v0_response(response)
      assert result.throttle_time_ms == nil
      assert length(result.partition_assignments) == 1

      [assignment] = result.partition_assignments
      assert %PartitionAssignment{topic: "test-topic", partitions: [0, 1, 2]} = assignment
    end

    test "returns error for non-zero error code" do
      response = %{
        error_code: 25,
        assignment: empty_member_assignment()
      }

      assert {:error, %Error{} = error} = ResponseHelpers.parse_v0_response(response)
      assert error.error == :unknown_member_id
      assert error.metadata == %{}
    end

    test "handles nil assignment in V0" do
      response = %{
        error_code: 0,
        assignment: nil
      }

      assert {:ok, %SyncGroup{} = result} = ResponseHelpers.parse_v0_response(response)
      assert result.partition_assignments == []
    end

    test "handles binary assignment in V0 (fallback to empty)" do
      response = %{
        error_code: 0,
        assignment: <<1, 2, 3>>
      }

      assert {:ok, %SyncGroup{} = result} = ResponseHelpers.parse_v0_response(response)
      assert result.partition_assignments == []
    end
  end

  # ---- parse_v1_plus_response/1 ----

  describe "parse_v1_plus_response/1" do
    test "parses successful response with throttle_time_ms" do
      response = %{
        throttle_time_ms: 100,
        error_code: 0,
        assignment: build_member_assignment([{"test-topic", [0, 1]}])
      }

      assert {:ok, %SyncGroup{} = result} = ResponseHelpers.parse_v1_plus_response(response)
      assert result.throttle_time_ms == 100
      assert length(result.partition_assignments) == 1
    end

    test "parses successful response with zero throttle_time_ms" do
      response = %{
        throttle_time_ms: 0,
        error_code: 0,
        assignment: empty_member_assignment()
      }

      assert {:ok, %SyncGroup{} = result} = ResponseHelpers.parse_v1_plus_response(response)
      assert result.throttle_time_ms == 0
      assert result.partition_assignments == []
    end

    test "returns error for non-zero error code (preserves no throttle_time_ms in error)" do
      response = %{
        throttle_time_ms: 50,
        error_code: 27,
        assignment: empty_member_assignment()
      }

      assert {:error, %Error{} = error} = ResponseHelpers.parse_v1_plus_response(response)
      assert error.error == :rebalance_in_progress
      assert error.metadata == %{}
    end

    test "handles nil assignment in V1+" do
      response = %{
        throttle_time_ms: 0,
        error_code: 0,
        assignment: nil
      }

      assert {:ok, %SyncGroup{} = result} = ResponseHelpers.parse_v1_plus_response(response)
      assert result.partition_assignments == []
    end

    test "handles binary assignment in V1+ (fallback to empty)" do
      response = %{
        throttle_time_ms: 10,
        error_code: 0,
        assignment: <<0, 0, 0, 0, 0, 0, 0, 0>>
      }

      assert {:ok, %SyncGroup{} = result} = ResponseHelpers.parse_v1_plus_response(response)
      assert result.partition_assignments == []
    end

    test "parses multiple topic assignments" do
      response = %{
        throttle_time_ms: 25,
        error_code: 0,
        assignment:
          build_member_assignment([
            {"topic-1", [0, 1]},
            {"topic-2", [2, 3, 4]},
            {"topic-3", [5]}
          ])
      }

      assert {:ok, %SyncGroup{} = result} = ResponseHelpers.parse_v1_plus_response(response)
      assert result.throttle_time_ms == 25
      assert length(result.partition_assignments) == 3
    end
  end

  # ---- parse_response/2 (generic with throttle extractor) ----

  describe "parse_response/2" do
    test "uses provided throttle_time extractor on success" do
      response = %{
        error_code: 0,
        assignment: empty_member_assignment(),
        custom_throttle: 42
      }

      extractor = fn resp -> resp.custom_throttle end

      assert {:ok, %SyncGroup{} = result} =
               ResponseHelpers.parse_response(response, extractor)

      assert result.throttle_time_ms == 42
    end

    test "calls throttle extractor with nil return" do
      response = %{
        error_code: 0,
        assignment: empty_member_assignment()
      }

      extractor = fn _resp -> nil end

      assert {:ok, %SyncGroup{} = result} =
               ResponseHelpers.parse_response(response, extractor)

      assert result.throttle_time_ms == nil
    end

    test "does not call throttle extractor on error path" do
      # The throttle extractor is NOT called on the error path because
      # parse_response returns early with {:error, ...}
      response = %{
        error_code: 22,
        assignment: empty_member_assignment()
      }

      extractor = fn _resp -> raise "should not be called" end

      assert {:error, %Error{} = error} =
               ResponseHelpers.parse_response(response, extractor)

      assert error.error == :illegal_generation
    end

    test "handles various error codes correctly" do
      error_codes = [
        {15, :coordinator_not_available},
        {16, :not_coordinator},
        {22, :illegal_generation},
        {25, :unknown_member_id},
        {27, :rebalance_in_progress},
        {30, :group_authorization_failed}
      ]

      for {code, expected_atom} <- error_codes do
        response = %{
          error_code: code,
          assignment: empty_member_assignment()
        }

        extractor = fn _resp -> nil end

        assert {:error, %Error{} = error} =
                 ResponseHelpers.parse_response(response, extractor),
               "expected error for code #{code}"

        assert error.error == expected_atom,
               "expected #{expected_atom} for code #{code}, got #{error.error}"
      end
    end

    test "error struct always has empty metadata map" do
      response = %{
        error_code: 25,
        assignment: empty_member_assignment()
      }

      extractor = fn _resp -> nil end

      {:error, error} = ResponseHelpers.parse_response(response, extractor)

      assert error.metadata == %{}
    end
  end
end
