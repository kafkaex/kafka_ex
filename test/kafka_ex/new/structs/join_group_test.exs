defmodule KafkaEx.New.Structs.JoinGroupTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Structs.JoinGroup
  alias KafkaEx.New.Structs.JoinGroup.Member

  describe "build/1" do
    test "builds join_group with all required fields" do
      result =
        JoinGroup.build(
          generation_id: 5,
          group_protocol: "assign",
          leader_id: "leader-123",
          member_id: "member-456"
        )

      assert %JoinGroup{
               generation_id: 5,
               group_protocol: "assign",
               leader_id: "leader-123",
               member_id: "member-456",
               throttle_time_ms: nil,
               members: []
             } = result
    end

    test "builds join_group with throttle_time_ms" do
      result =
        JoinGroup.build(
          generation_id: 10,
          group_protocol: "roundrobin",
          leader_id: "leader-1",
          member_id: "member-1",
          throttle_time_ms: 100
        )

      assert %JoinGroup{throttle_time_ms: 100} = result
    end

    test "builds join_group with zero throttle_time_ms" do
      result =
        JoinGroup.build(
          generation_id: 1,
          group_protocol: "assign",
          leader_id: "leader",
          member_id: "member",
          throttle_time_ms: 0
        )

      assert result.throttle_time_ms == 0
    end

    test "builds join_group with nil when throttle_time_ms not provided" do
      result =
        JoinGroup.build(
          generation_id: 1,
          group_protocol: "assign",
          leader_id: "leader",
          member_id: "member"
        )

      assert result.throttle_time_ms == nil
    end

    test "builds join_group with members list" do
      members = [
        %Member{member_id: "member-1", member_metadata: <<0, 1, 2, 3>>},
        %Member{member_id: "member-2", member_metadata: <<4, 5, 6, 7>>}
      ]

      result =
        JoinGroup.build(
          generation_id: 3,
          group_protocol: "assign",
          leader_id: "member-1",
          member_id: "member-1",
          members: members
        )

      assert %JoinGroup{members: ^members} = result
      assert length(result.members) == 2
    end

    test "builds join_group with empty members when not provided" do
      result =
        JoinGroup.build(
          generation_id: 1,
          group_protocol: "assign",
          leader_id: "leader",
          member_id: "member"
        )

      assert result.members == []
    end

    test "builds join_group with all fields" do
      members = [
        %Member{member_id: "member-1", member_metadata: <<0, 1, 2>>}
      ]

      result =
        JoinGroup.build(
          throttle_time_ms: 50,
          generation_id: 7,
          group_protocol: "range",
          leader_id: "leader-999",
          member_id: "member-888",
          members: members
        )

      assert %JoinGroup{
               throttle_time_ms: 50,
               generation_id: 7,
               group_protocol: "range",
               leader_id: "leader-999",
               member_id: "member-888",
               members: ^members
             } = result
    end

    test "builds join_group ignoring extra options" do
      result =
        JoinGroup.build(
          generation_id: 1,
          group_protocol: "assign",
          leader_id: "leader",
          member_id: "member",
          extra: "ignored"
        )

      assert %JoinGroup{generation_id: 1} = result
      refute Map.has_key?(result, :extra)
    end

    test "raises when generation_id is missing" do
      assert_raise KeyError, fn ->
        JoinGroup.build(
          group_protocol: "assign",
          leader_id: "leader",
          member_id: "member"
        )
      end
    end

    test "raises when group_protocol is missing" do
      assert_raise KeyError, fn ->
        JoinGroup.build(
          generation_id: 1,
          leader_id: "leader",
          member_id: "member"
        )
      end
    end

    test "raises when leader_id is missing" do
      assert_raise KeyError, fn ->
        JoinGroup.build(
          generation_id: 1,
          group_protocol: "assign",
          member_id: "member"
        )
      end
    end

    test "raises when member_id is missing" do
      assert_raise KeyError, fn ->
        JoinGroup.build(
          generation_id: 1,
          group_protocol: "assign",
          leader_id: "leader"
        )
      end
    end
  end

  describe "leader?/1" do
    test "returns true when member_id equals leader_id" do
      join_group =
        JoinGroup.build(
          generation_id: 1,
          group_protocol: "assign",
          leader_id: "member-123",
          member_id: "member-123"
        )

      assert JoinGroup.leader?(join_group) == true
    end

    test "returns false when member_id does not equal leader_id" do
      join_group =
        JoinGroup.build(
          generation_id: 1,
          group_protocol: "assign",
          leader_id: "member-123",
          member_id: "member-456"
        )

      assert JoinGroup.leader?(join_group) == false
    end

    test "returns false when leader_id is different" do
      join_group =
        JoinGroup.build(
          generation_id: 1,
          group_protocol: "assign",
          leader_id: "leader",
          member_id: "follower"
        )

      refute JoinGroup.leader?(join_group)
    end
  end

  describe "Member struct" do
    test "creates member with member_id and member_metadata" do
      metadata = <<0, 1, 2, 3, 4>>
      member = %Member{member_id: "member-1", member_metadata: metadata}

      assert member.member_id == "member-1"
      assert member.member_metadata == metadata
    end

    test "creates member with nil fields by default" do
      member = %Member{}

      assert member.member_id == nil
      assert member.member_metadata == nil
    end
  end
end
