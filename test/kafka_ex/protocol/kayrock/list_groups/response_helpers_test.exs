defmodule KafkaEx.Protocol.Kayrock.ListGroups.ResponseHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.ListGroups.ResponseHelpers
  alias KafkaEx.Messages.ConsumerGroupListing

  test "maps groups to ConsumerGroupListing structs when error_code is 0" do
    response = %{
      error_code: 0,
      groups: [
        %{group_id: "g1", protocol_type: "consumer"},
        %{group_id: "g2", protocol_type: "connect"}
      ]
    }

    assert ResponseHelpers.parse_response(response) ==
             {:ok,
              [
                %ConsumerGroupListing{group_id: "g1", protocol_type: "consumer"},
                %ConsumerGroupListing{group_id: "g2", protocol_type: "connect"}
              ]}
  end

  test "returns {:ok, []} when there are no groups" do
    assert ResponseHelpers.parse_response(%{error_code: 0, groups: []}) == {:ok, []}
  end

  test "returns {:error, %KafkaEx.Client.Error{}} for a non-zero top-level error_code" do
    # 15 = coordinator_not_available
    assert {:error, %KafkaEx.Client.Error{error: :coordinator_not_available}} =
             ResponseHelpers.parse_response(%{error_code: 15, groups: []})
  end
end
