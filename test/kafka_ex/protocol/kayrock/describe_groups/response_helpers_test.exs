defmodule KafkaEx.Protocol.Kayrock.DescribeGroups.ResponseHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.DescribeGroups.ResponseHelpers
  alias KafkaEx.Messages.ConsumerGroupDescription

  @base_group %{
    group_id: "test-group",
    error_code: 0,
    group_state: "Stable",
    protocol_type: "consumer",
    protocol_data: "range",
    members: [
      %{
        member_id: "member-1",
        client_id: "client-1",
        client_host: "/127.0.0.1",
        member_metadata: <<>>,
        member_assignment: nil
      }
    ]
  }

  describe "parse_response/1" do
    test "returns ok with parsed groups on success" do
      response = %{groups: [@base_group]}

      assert {:ok, [group]} = ResponseHelpers.parse_response(response)
      assert %ConsumerGroupDescription{} = group
      assert group.group_id == "test-group"
      assert group.state == "Stable"
      assert group.protocol_type == "consumer"
      assert group.protocol == "range"
    end

    test "returns error list when any group has non-zero error_code" do
      response = %{
        groups: [
          %{group_id: "ok-group", error_code: 0},
          %{group_id: "bad-group", error_code: 15}
        ]
      }

      assert {:error, [{"bad-group", :coordinator_not_available}]} =
               ResponseHelpers.parse_response(response)
    end

    test "returns multiple errors for multiple failed groups" do
      response = %{
        groups: [
          %{group_id: "bad1", error_code: 25},
          %{group_id: "bad2", error_code: 16}
        ]
      }

      assert {:error, error_list} = ResponseHelpers.parse_response(response)
      assert length(error_list) == 2
      assert {"bad1", :unknown_member_id} in error_list
      assert {"bad2", :not_coordinator} in error_list
    end

    test "returns ok with empty list for empty groups" do
      response = %{groups: []}
      assert {:ok, []} = ResponseHelpers.parse_response(response)
    end

    test "handles multiple successful groups" do
      group2 = %{@base_group | group_id: "group-2"}
      response = %{groups: [@base_group, group2]}

      assert {:ok, groups} = ResponseHelpers.parse_response(response)
      assert length(groups) == 2
      assert Enum.map(groups, & &1.group_id) == ["test-group", "group-2"]
    end

    test "passes authorized_operations through to domain struct" do
      group_with_auth = Map.put(@base_group, :authorized_operations, 2047)
      response = %{groups: [group_with_auth]}

      assert {:ok, [group]} = ResponseHelpers.parse_response(response)
      assert group.authorized_operations == 2047
    end

    test "passes nil authorized_operations when not present" do
      response = %{groups: [@base_group]}

      assert {:ok, [group]} = ResponseHelpers.parse_response(response)
      assert group.authorized_operations == nil
    end

    test "passes group_instance_id through to member domain struct" do
      group_with_instance_id =
        Map.update!(@base_group, :members, fn members ->
          Enum.map(members, &Map.put(&1, :group_instance_id, "static-instance-1"))
        end)

      response = %{groups: [group_with_instance_id]}

      assert {:ok, [group]} = ResponseHelpers.parse_response(response)
      [member] = group.members
      assert member.group_instance_id == "static-instance-1"
    end

    test "passes nil group_instance_id when not present" do
      response = %{groups: [@base_group]}

      assert {:ok, [group]} = ResponseHelpers.parse_response(response)
      [member] = group.members
      assert member.group_instance_id == nil
    end

    test "only reports groups with non-zero error codes" do
      response = %{
        groups: [
          %{group_id: "ok1", error_code: 0},
          %{group_id: "bad1", error_code: 27},
          %{group_id: "ok2", error_code: 0}
        ]
      }

      # When ANY group has an error, only errors are reported
      assert {:error, [{"bad1", :rebalance_in_progress}]} =
               ResponseHelpers.parse_response(response)
    end
  end
end
