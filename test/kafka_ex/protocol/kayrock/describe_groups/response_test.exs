defmodule KafkaEx.Protocol.Kayrock.DescribeGroups.ResponseTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.DescribeGroups
  alias KafkaEx.Messages.ConsumerGroupDescription
  alias KafkaEx.Messages.ConsumerGroupDescription.Member
  alias KafkaEx.Messages.ConsumerGroupDescription.Member.MemberAssignment
  alias KafkaEx.Messages.ConsumerGroupDescription.Member.MemberAssignment.PartitionAssignment

  # ---- Shared test data ----

  # Base group data shared by all versions (V0-V2)
  @base_group_data %{
    group_id: "succeeded",
    error_code: 0,
    group_state: "stable",
    protocol_type: "protocol_type",
    protocol_data: "protocol",
    members: [
      %{
        member_id: "member_id",
        client_id: "client_id",
        client_host: "client_host",
        member_metadata: "member_metadata",
        member_assignment: %{
          version: 0,
          user_data: "user_data",
          partition_assignments: [
            %{topic: "test-topic", partitions: [1, 2, 3]}
          ]
        }
      }
    ]
  }

  @expected_group %ConsumerGroupDescription{
    group_id: "succeeded",
    state: "stable",
    protocol_type: "protocol_type",
    protocol: "protocol",
    authorized_operations: nil,
    members: [
      %Member{
        member_id: "member_id",
        client_id: "client_id",
        client_host: "client_host",
        member_metadata: "member_metadata",
        group_instance_id: nil,
        member_assignment: %MemberAssignment{
          version: 0,
          user_data: "user_data",
          partition_assignments: [
            %PartitionAssignment{
              topic: "test-topic",
              partitions: [1, 2, 3]
            }
          ]
        }
      }
    ]
  }

  # ---- V0 Response ----

  describe "V0 Response implementation" do
    test "returns response if all groups succeeded" do
      response = %Kayrock.DescribeGroups.V0.Response{groups: [@base_group_data]}

      assert {:ok, [@expected_group]} ==
               DescribeGroups.Response.parse_response(response)
    end

    test "returns error if any group failed" do
      response = %Kayrock.DescribeGroups.V0.Response{
        groups: [
          %{group_id: "succeeded", error_code: 0},
          %{group_id: "failed", error_code: 1}
        ]
      }

      assert {:error, [{"failed", :offset_out_of_range}]} ==
               DescribeGroups.Response.parse_response(response)
    end

    test "returns multiple errors if multiple groups failed" do
      response = %Kayrock.DescribeGroups.V0.Response{
        groups: [
          %{group_id: "failed1", error_code: 15},
          %{group_id: "failed2", error_code: 16}
        ]
      }

      assert {:error, error_list} = DescribeGroups.Response.parse_response(response)
      assert {"failed1", :coordinator_not_available} in error_list
      assert {"failed2", :not_coordinator} in error_list
    end

    test "handles empty groups list" do
      response = %Kayrock.DescribeGroups.V0.Response{groups: []}
      assert {:ok, []} == DescribeGroups.Response.parse_response(response)
    end
  end

  # ---- V1 Response ----

  describe "V1 Response implementation" do
    test "returns response if all groups succeeded" do
      response = %Kayrock.DescribeGroups.V1.Response{
        throttle_time_ms: 100,
        groups: [@base_group_data]
      }

      assert {:ok, [@expected_group]} ==
               DescribeGroups.Response.parse_response(response)
    end

    test "returns error if any group failed" do
      response = %Kayrock.DescribeGroups.V1.Response{
        groups: [
          %{group_id: "succeeded", error_code: 0},
          %{group_id: "failed", error_code: 1}
        ]
      }

      assert {:error, [{"failed", :offset_out_of_range}]} ==
               DescribeGroups.Response.parse_response(response)
    end
  end

  # ---- V2 Response ----

  describe "V2 Response implementation" do
    test "returns response if all groups succeeded (schema-identical to V1)" do
      response = %Kayrock.DescribeGroups.V2.Response{
        throttle_time_ms: 0,
        groups: [@base_group_data]
      }

      assert {:ok, [@expected_group]} ==
               DescribeGroups.Response.parse_response(response)
    end

    test "returns error if any group failed" do
      response = %Kayrock.DescribeGroups.V2.Response{
        groups: [
          %{group_id: "failed", error_code: 25}
        ]
      }

      assert {:error, [{"failed", :unknown_member_id}]} ==
               DescribeGroups.Response.parse_response(response)
    end
  end

  # ---- V3 Response ----

  describe "V3 Response implementation" do
    test "returns response with authorized_operations per group" do
      group_data = Map.put(@base_group_data, :authorized_operations, 2047)

      response = %Kayrock.DescribeGroups.V3.Response{
        throttle_time_ms: 0,
        groups: [group_data]
      }

      assert {:ok, [group]} = DescribeGroups.Response.parse_response(response)
      assert group.authorized_operations == 2047
      assert group.group_id == "succeeded"
    end

    test "returns nil authorized_operations when not present in group" do
      response = %Kayrock.DescribeGroups.V3.Response{
        throttle_time_ms: 0,
        groups: [@base_group_data]
      }

      assert {:ok, [group]} = DescribeGroups.Response.parse_response(response)
      assert group.authorized_operations == nil
    end

    test "returns error if any group failed" do
      response = %Kayrock.DescribeGroups.V3.Response{
        groups: [
          %{group_id: "failed", error_code: 30}
        ]
      }

      assert {:error, [{"failed", :group_authorization_failed}]} ==
               DescribeGroups.Response.parse_response(response)
    end
  end

  # ---- V4 Response ----

  describe "V4 Response implementation" do
    test "returns response with group_instance_id per member" do
      group_data =
        @base_group_data
        |> Map.put(:authorized_operations, 1023)
        |> Map.update!(:members, fn members ->
          Enum.map(members, &Map.put(&1, :group_instance_id, "static-instance-1"))
        end)

      response = %Kayrock.DescribeGroups.V4.Response{
        throttle_time_ms: 50,
        groups: [group_data]
      }

      assert {:ok, [group]} = DescribeGroups.Response.parse_response(response)
      assert group.authorized_operations == 1023

      [member] = group.members
      assert member.group_instance_id == "static-instance-1"
    end

    test "returns nil group_instance_id for dynamic membership" do
      group_data =
        @base_group_data
        |> Map.put(:authorized_operations, 0)
        |> Map.update!(:members, fn members ->
          Enum.map(members, &Map.put(&1, :group_instance_id, nil))
        end)

      response = %Kayrock.DescribeGroups.V4.Response{
        throttle_time_ms: 0,
        groups: [group_data]
      }

      assert {:ok, [group]} = DescribeGroups.Response.parse_response(response)

      [member] = group.members
      assert member.group_instance_id == nil
    end

    test "returns error if any group failed" do
      response = %Kayrock.DescribeGroups.V4.Response{
        groups: [
          %{group_id: "failed", error_code: 27}
        ]
      }

      assert {:error, [{"failed", :rebalance_in_progress}]} ==
               DescribeGroups.Response.parse_response(response)
    end
  end

  # ---- V5 Response (FLEX) ----

  describe "V5 Response implementation (FLEX)" do
    test "returns response with group_instance_id and authorized_operations" do
      group_data =
        @base_group_data
        |> Map.put(:authorized_operations, 511)
        |> Map.put(:tagged_fields, [])
        |> Map.update!(:members, fn members ->
          Enum.map(members, fn m ->
            m
            |> Map.put(:group_instance_id, "static-flex-1")
            |> Map.put(:tagged_fields, [])
          end)
        end)

      response = %Kayrock.DescribeGroups.V5.Response{
        throttle_time_ms: 200,
        groups: [group_data],
        tagged_fields: []
      }

      assert {:ok, [group]} = DescribeGroups.Response.parse_response(response)
      assert group.authorized_operations == 511

      [member] = group.members
      assert member.group_instance_id == "static-flex-1"
    end

    test "returns response with tagged_fields present" do
      group_data =
        @base_group_data
        |> Map.put(:authorized_operations, 0)
        |> Map.put(:tagged_fields, [{0, <<1, 2, 3>>}])
        |> Map.update!(:members, fn members ->
          Enum.map(members, fn m ->
            m
            |> Map.put(:group_instance_id, nil)
            |> Map.put(:tagged_fields, [{0, <<4, 5>>}])
          end)
        end)

      response = %Kayrock.DescribeGroups.V5.Response{
        throttle_time_ms: 0,
        groups: [group_data],
        tagged_fields: [{0, <<6, 7, 8>>}]
      }

      # tagged_fields at all levels are ignored in domain layer
      assert {:ok, [group]} = DescribeGroups.Response.parse_response(response)
      assert group.group_id == "succeeded"
    end

    test "returns error if any group failed" do
      response = %Kayrock.DescribeGroups.V5.Response{
        groups: [
          %{group_id: "failed", error_code: 22}
        ],
        tagged_fields: []
      }

      assert {:error, [{"failed", :illegal_generation}]} ==
               DescribeGroups.Response.parse_response(response)
    end
  end

  # ---- Cross-version consistency ----

  describe "Cross-version response consistency (V0-V5)" do
    test "V0-V2 produce identical domain results (no authorized_operations, no group_instance_id)" do
      responses = [
        %Kayrock.DescribeGroups.V0.Response{groups: [@base_group_data]},
        %Kayrock.DescribeGroups.V1.Response{throttle_time_ms: 100, groups: [@base_group_data]},
        %Kayrock.DescribeGroups.V2.Response{throttle_time_ms: 0, groups: [@base_group_data]}
      ]

      for response <- responses do
        assert {:ok, [group]} = DescribeGroups.Response.parse_response(response)
        assert group.group_id == "succeeded"
        assert group.authorized_operations == nil

        [member] = group.members
        assert member.group_instance_id == nil
      end
    end

    test "V3-V5 produce identical domain results with authorized_operations" do
      group_with_auth_ops = Map.put(@base_group_data, :authorized_operations, 2047)

      responses = [
        %Kayrock.DescribeGroups.V3.Response{
          throttle_time_ms: 0,
          groups: [group_with_auth_ops]
        },
        %Kayrock.DescribeGroups.V4.Response{
          throttle_time_ms: 50,
          groups: [group_with_auth_ops]
        },
        %Kayrock.DescribeGroups.V5.Response{
          throttle_time_ms: 100,
          groups: [group_with_auth_ops],
          tagged_fields: []
        }
      ]

      for response <- responses do
        assert {:ok, [group]} = DescribeGroups.Response.parse_response(response)
        assert group.authorized_operations == 2047
      end
    end

    test "error responses are identical between all versions" do
      responses = [
        %Kayrock.DescribeGroups.V0.Response{
          groups: [%{group_id: "failed", error_code: 15}]
        },
        %Kayrock.DescribeGroups.V1.Response{
          throttle_time_ms: 0,
          groups: [%{group_id: "failed", error_code: 15}]
        },
        %Kayrock.DescribeGroups.V2.Response{
          throttle_time_ms: 0,
          groups: [%{group_id: "failed", error_code: 15}]
        },
        %Kayrock.DescribeGroups.V3.Response{
          throttle_time_ms: 0,
          groups: [%{group_id: "failed", error_code: 15}]
        },
        %Kayrock.DescribeGroups.V4.Response{
          throttle_time_ms: 0,
          groups: [%{group_id: "failed", error_code: 15}]
        },
        %Kayrock.DescribeGroups.V5.Response{
          throttle_time_ms: 0,
          groups: [%{group_id: "failed", error_code: 15}],
          tagged_fields: []
        }
      ]

      for response <- responses do
        assert {:error, [{"failed", :coordinator_not_available}]} ==
                 DescribeGroups.Response.parse_response(response)
      end
    end
  end

  # ---- Any fallback ----

  describe "Any fallback Response implementation" do
    test "parses plain map response like V0" do
      response = %{
        groups: [
          %{
            group_id: "test-group",
            error_code: 0,
            group_state: "Stable",
            protocol_type: "consumer",
            protocol_data: "range",
            members: []
          }
        ]
      }

      assert {:ok, [group]} = DescribeGroups.Response.parse_response(response)
      assert group.group_id == "test-group"
      assert group.state == "Stable"
      assert group.members == []
    end

    test "parses plain map response with authorized_operations" do
      response = %{
        groups: [
          %{
            group_id: "test-group",
            error_code: 0,
            group_state: "Stable",
            protocol_type: "consumer",
            protocol_data: "range",
            authorized_operations: 255,
            members: []
          }
        ]
      }

      assert {:ok, [group]} = DescribeGroups.Response.parse_response(response)
      assert group.authorized_operations == 255
    end

    test "handles error response via Any path" do
      response = %{
        groups: [
          %{group_id: "failed", error_code: 25}
        ]
      }

      assert {:error, [{"failed", :unknown_member_id}]} ==
               DescribeGroups.Response.parse_response(response)
    end

    test "handles empty groups via Any path" do
      response = %{groups: []}
      assert {:ok, []} == DescribeGroups.Response.parse_response(response)
    end
  end
end
