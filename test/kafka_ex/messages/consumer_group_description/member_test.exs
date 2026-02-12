defmodule KafkaEx.Messages.ConsumerGroupDescription.MemberTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Messages.ConsumerGroupDescription.Member

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

  describe "from_describe_group_response/1 with binary member_assignment" do
    test "deserializes ConsumerProtocol Assignment binary" do
      # Binary from Kayrock DescribeGroups: version=0, 1 topic "test-topic" with partition 0, empty user_data
      topic = "test-topic"
      topic_len = byte_size(topic)

      binary =
        <<0::16, 0, 0, 0, 1, topic_len::16, topic::binary, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0>>

      response = %{
        member_id: "member-1",
        client_id: "client-1",
        client_host: "/127.0.0.1",
        member_metadata: <<>>,
        member_assignment: binary
      }

      result = Member.from_describe_group_response(response)

      assert result.member_id == "member-1"
      assignment = result.member_assignment
      assert assignment.version == 0
      assert assignment.user_data == <<>>
      assert length(assignment.partition_assignments) == 1

      pa = hd(assignment.partition_assignments)
      assert pa.topic == topic
      assert pa.partitions == [0]
    end

    test "handles empty binary member_assignment" do
      response = %{
        member_id: "member-1",
        client_id: "client-1",
        client_host: "/127.0.0.1",
        member_metadata: <<>>,
        member_assignment: ""
      }

      result = Member.from_describe_group_response(response)
      assert result.member_assignment == nil
    end
  end

  describe "accessor functions" do
    setup do
      member = %Member{
        member_id: "test-member",
        client_id: "test-client",
        client_host: "/192.168.1.1",
        member_metadata: "metadata",
        member_assignment: nil
      }

      {:ok, %{member: member}}
    end

    test "member_id/1 returns member id", %{member: member} do
      assert Member.member_id(member) == "test-member"
    end

    test "client_id/1 returns client id", %{member: member} do
      assert Member.client_id(member) == "test-client"
    end

    test "client_host/1 returns client host", %{member: member} do
      assert Member.client_host(member) == "/192.168.1.1"
    end

    test "assignment/1 returns member assignment", %{member: member} do
      assert Member.assignment(member) == nil
    end
  end
end
