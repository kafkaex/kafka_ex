defmodule KafkaEx.New.Client.RequestBuilderTest do
  use ExUnit.Case, async: true
  alias KafkaEx.New.Client.RequestBuilder

  describe "describe_groups_request/2" do
    test "returns request for DescribeGroups API" do
      state = %KafkaEx.New.Client.State{api_versions: %{describe_groups: 1}}
      group_names = ["group1", "group2"]

      assert RequestBuilder.describe_groups_request(group_names, state) ==
               {:ok,
                %KafkaEx.New.Protocols.DescribeGroups.Request{
                  group_names: group_names
                }}
    end
  end
end
