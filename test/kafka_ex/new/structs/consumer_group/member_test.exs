defmodule KafkaEx.New.Structs.ConsumerGroup.MemberTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Structs.ConsumerGroup.Member

  describe "from_describe_group_response/1" do
    test "returns a Member struct" do
      response = %{
        member_id: "member_id",
        client_id: "client_id",
        client_host: "client_host",
        member_metadata: "member_metadata",
        member_assignment: nil
      }

      result = Member.from_describe_group_response(response)

      assert "member_id" == result.member_id
      assert "client_id" == result.client_id
      assert "client_host" == result.client_host
      assert "member_metadata" == result.member_metadata
      assert nil == result.member_assignment
    end

    test "returns a Member struct with member_assignment" do
      response = %{
        member_id: "member_id",
        client_id: "client_id",
        client_host: "client_host",
        member_metadata: "member_metadata",
        member_assignment: %{
          version: 0,
          user_data: "user_data",
          partition_assignments: []
        }
      }

      result = Member.from_describe_group_response(response)

      assert "member_id" == result.member_id
      assert "client_id" == result.client_id
      assert "client_host" == result.client_host
      assert "member_metadata" == result.member_metadata

      member_assignment = result.member_assignment
      assert 0 == member_assignment.version
      assert "user_data" == member_assignment.user_data
      assert [] == member_assignment.partition_assignments
    end

    test "returns a Member struct with member_assignment and partition_assignments" do
      response = %{
        member_id: "member_id",
        client_id: "client_id",
        client_host: "client_host",
        member_metadata: "member_metadata",
        member_assignment: %{
          version: 0,
          user_data: "user_data",
          partition_assignments: [
            %{
              topic: "topic",
              partitions: [0, 1]
            }
          ]
        }
      }

      result = Member.from_describe_group_response(response)

      assert "member_id" == result.member_id
      assert "client_id" == result.client_id
      assert "client_host" == result.client_host
      assert "member_metadata" == result.member_metadata

      member_assignment = result.member_assignment
      assert 0 == member_assignment.version
      assert "user_data" == member_assignment.user_data
      assert 1 == length(member_assignment.partition_assignments)

      partition_assignment = Enum.at(member_assignment.partition_assignments, 0)
      assert "topic" == partition_assignment.topic
      assert [0, 1] == partition_assignment.partitions
    end
  end
end
