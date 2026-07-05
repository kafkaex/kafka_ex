defmodule KafkaEx.Messages.ConsumerGroupListingTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Messages.ConsumerGroupListing

  describe "from_list_groups_response/1" do
    test "builds a listing from a Kayrock group map (group_id + protocol_type)" do
      group = %{group_id: "my-group", protocol_type: "consumer"}

      assert ConsumerGroupListing.from_list_groups_response(group) ==
               %ConsumerGroupListing{group_id: "my-group", protocol_type: "consumer"}
    end

    test "ignores extra keys such as tagged_fields (V3 entries)" do
      group = %{group_id: "g", protocol_type: "consumer", tagged_fields: []}

      assert ConsumerGroupListing.from_list_groups_response(group) ==
               %ConsumerGroupListing{group_id: "g", protocol_type: "consumer"}
    end
  end

  describe "accessors" do
    test "group_id/1 and protocol_type/1 return their fields" do
      listing = %ConsumerGroupListing{group_id: "g", protocol_type: "connect"}

      assert ConsumerGroupListing.group_id(listing) == "g"
      assert ConsumerGroupListing.protocol_type(listing) == "connect"
    end
  end
end
