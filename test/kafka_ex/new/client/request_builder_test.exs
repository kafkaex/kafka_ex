defmodule KafkaEx.New.Client.RequestBuilderTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Client.RequestBuilder

  describe "describe_groups_request/2" do
    test "returns request for DescribeGroups API" do
      state = %KafkaEx.New.Client.State{api_versions: %{describe_groups: 1}}
      group_names = ["group1", "group2"]

      expected_request = %Kayrock.DescribeGroups.V1.Request{
        group_ids: group_names
      }

      request = RequestBuilder.describe_groups_request(group_names, state)

      assert expected_request == request
    end
  end
end
