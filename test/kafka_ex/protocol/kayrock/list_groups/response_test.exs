defmodule KafkaEx.Protocol.Kayrock.ListGroups.ResponseTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.ListGroups
  alias KafkaEx.Messages.ConsumerGroupListing

  @groups [
    %{group_id: "succeeded", protocol_type: "consumer"}
  ]

  @expected [%ConsumerGroupListing{group_id: "succeeded", protocol_type: "consumer"}]

  describe "V0 Response implementation" do
    test "parses groups (no throttle_time_ms)" do
      response = %Kayrock.ListGroups.V0.Response{error_code: 0, groups: @groups}
      assert ListGroups.Response.parse_response(response) == {:ok, @expected}
    end

    test "surfaces a non-zero top-level error_code" do
      response = %Kayrock.ListGroups.V0.Response{error_code: 15, groups: []}
      assert ListGroups.Response.parse_response(response) == {:error, :coordinator_not_available}
    end
  end

  describe "V1/V2 Response implementations (throttle_time_ms ignored)" do
    test "V1 parses groups and ignores throttle_time_ms" do
      response = %Kayrock.ListGroups.V1.Response{error_code: 0, throttle_time_ms: 5, groups: @groups}
      assert ListGroups.Response.parse_response(response) == {:ok, @expected}
    end

    test "V2 parses groups and ignores throttle_time_ms" do
      response = %Kayrock.ListGroups.V2.Response{error_code: 0, throttle_time_ms: 7, groups: @groups}
      assert ListGroups.Response.parse_response(response) == {:ok, @expected}
    end
  end

  describe "V3 Response implementation (FLEX)" do
    test "parses groups and ignores throttle_time_ms/tagged_fields" do
      groups = [%{group_id: "succeeded", protocol_type: "consumer", tagged_fields: []}]

      response = %Kayrock.ListGroups.V3.Response{
        error_code: 0,
        throttle_time_ms: 0,
        groups: groups,
        tagged_fields: []
      }

      assert ListGroups.Response.parse_response(response) == {:ok, @expected}
    end
  end

  describe "Any fallback Response implementation" do
    test "parses an unknown-version response map via the shared helper" do
      response = %{error_code: 0, groups: @groups}
      assert ListGroups.Response.parse_response(response) == {:ok, @expected}
    end
  end
end
