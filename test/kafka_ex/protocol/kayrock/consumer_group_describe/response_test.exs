defmodule KafkaEx.Protocol.Kayrock.ConsumerGroupDescribe.ResponseTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Messages.ConsumerGroupDescription
  alias KafkaEx.Messages.ConsumerGroupDescription.Member
  alias KafkaEx.Messages.ConsumerGroupDescription.Member.MemberAssignment
  alias KafkaEx.Messages.ConsumerGroupDescription.Member.MemberAssignment.PartitionAssignment
  alias KafkaEx.Protocol.Kayrock.ConsumerGroupDescribe

  @t1 <<1::128>>

  defp member(extra) do
    Map.merge(
      %{
        member_id: "m-1",
        instance_id: nil,
        rack_id: "rack-a",
        member_epoch: 4,
        client_id: "client",
        client_host: "/127.0.0.1",
        subscribed_topic_names: ["orders"],
        subscribed_topic_regex: nil,
        assignment: %{topic_partitions: [%{topic_id: @t1, topic_name: "orders", partitions: [0, 1]}]},
        target_assignment: %{topic_partitions: [%{topic_id: @t1, topic_name: "orders", partitions: [0]}]},
        tagged_fields: []
      },
      extra
    )
  end

  defp group(extra) do
    Map.merge(
      %{
        error_code: 0,
        error_message: nil,
        group_id: "g",
        group_state: "Stable",
        group_epoch: 5,
        assignment_epoch: 5,
        assignor_name: "uniform",
        members: [],
        authorized_operations: -2_147_483_648,
        tagged_fields: []
      },
      extra
    )
  end

  defp response(0, groups), do: %Kayrock.ConsumerGroupDescribe.V0.Response{throttle_time_ms: 0, groups: groups}
  defp response(1, groups), do: %Kayrock.ConsumerGroupDescribe.V1.Response{throttle_time_ms: 0, groups: groups}

  for version <- [0, 1] do
    describe "V#{version} Response" do
      @version version

      test "parses group-level fields" do
        assert {:ok, [%ConsumerGroupDescription{} = desc]} =
                 ConsumerGroupDescribe.Response.parse_response(response(@version, [group(%{})]))

        assert desc.group_id == "g"
        assert desc.state == "Stable"
        assert desc.type == :consumer
        assert desc.protocol_type == "consumer"
        assert desc.protocol == "uniform"
        assert desc.group_epoch == 5
        assert desc.assignment_epoch == 5
        assert desc.authorized_operations == -2_147_483_648
        assert desc.members == []
        assert ConsumerGroupDescription.stable?(desc)
      end

      test "parses members with assignment and target assignment" do
        member = if @version == 1, do: member(%{member_type: 1}), else: member(%{})

        assert {:ok, [desc]} =
                 ConsumerGroupDescribe.Response.parse_response(response(@version, [group(%{members: [member]})]))

        assert [%Member{} = m] = desc.members
        assert m.member_id == "m-1"
        assert m.client_id == "client"
        assert m.client_host == "/127.0.0.1"
        assert m.member_metadata == nil
        assert m.member_epoch == 4
        assert m.rack_id == "rack-a"
        assert m.group_instance_id == nil
        assert m.subscribed_topic_names == ["orders"]
        assert m.subscribed_topic_regex == nil

        assert %MemberAssignment{partition_assignments: [current]} = Member.assignment(m)
        assert current == %PartitionAssignment{topic: "orders", topic_id: @t1, partitions: [0, 1]}

        assert %MemberAssignment{partition_assignments: [target]} = m.target_assignment
        assert target == %PartitionAssignment{topic: "orders", topic_id: @t1, partitions: [0]}

        if @version == 1, do: assert(m.member_type == :consumer), else: assert(m.member_type == nil)
      end

      test "returns {:error, [{group_id, atom}]} for failed groups" do
        groups = [group(%{group_id: "ok"}), group(%{group_id: "missing", error_code: 69, error_message: "nope"})]

        assert {:error, [{"missing", :group_id_not_found}]} =
                 ConsumerGroupDescribe.Response.parse_response(response(@version, groups))
      end

      test "coordinator errors are reported per group" do
        assert {:error, [{"g", :not_coordinator}]} =
                 ConsumerGroupDescribe.Response.parse_response(response(@version, [group(%{error_code: 16})]))
      end
    end
  end

  test "V1 member_type values" do
    for {code, type} <- [{-1, :unknown}, {0, :classic}, {1, :consumer}] do
      {:ok, [desc]} =
        ConsumerGroupDescribe.Response.parse_response(response(1, [group(%{members: [member(%{member_type: code})]})]))

      assert [%Member{member_type: ^type}] = desc.members
    end
  end

  test "matches the pre-refactor parser (DescribeGroups envelope + KIP-848 builder)" do
    # The parser used to be a copy of DescribeGroups.ResponseHelpers.parse_response/1
    # with a different builder; this is that copy, kept as the reference.
    reference = fn %{groups: groups} ->
      case Enum.filter(groups, &(&1.error_code != 0)) do
        [] ->
          {:ok, Enum.map(groups, &ConsumerGroupDescription.from_consumer_group_describe_response/1)}

        errors ->
          {:error,
           Enum.map(errors, fn %{group_id: id, error_code: code} -> {id, Kayrock.ErrorCode.code_to_atom(code)} end)}
      end
    end

    cases = [
      [],
      [group(%{})],
      [group(%{group_id: "a", members: [member(%{})]}), group(%{group_id: "b"})],
      [group(%{group_id: "a"}), group(%{group_id: "b", error_code: 69}), group(%{group_id: "c", error_code: 16})],
      [group(%{group_id: "x", error_code: 15})]
    ]

    for groups <- cases, version <- [0, 1] do
      response = response(version, groups)
      assert ConsumerGroupDescribe.Response.parse_response(response) == reference.(response)
    end
  end

  test "Any fallback parses a plain map" do
    assert {:ok, [%ConsumerGroupDescription{group_id: "g"}]} =
             ConsumerGroupDescribe.Response.parse_response(%{groups: [group(%{})]})
  end

  test "classic DescribeGroups results leave the new fields nil" do
    desc =
      ConsumerGroupDescription.from_describe_group_response(%{
        group_id: "g",
        group_state: "Stable",
        protocol_type: "consumer",
        protocol_data: "range",
        members: [%{member_id: "m", client_id: "c", client_host: "h", member_metadata: "", member_assignment: nil}]
      })

    assert desc.type == nil
    assert desc.group_epoch == nil
    assert desc.assignment_epoch == nil
    assert [%Member{member_epoch: nil, target_assignment: nil, member_type: nil}] = desc.members
  end
end
