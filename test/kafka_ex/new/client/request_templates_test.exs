defmodule KafkaEx.New.Client.RequestTemplatesTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Client.RequestTemplates

  setup do
    state = %KafkaEx.New.Client.State{
      api_versions: %{
        2 => {0, 2},
        15 => {0, 1}
      }
    }

    %{state: state}
  end

  describe "request_template/2 - describe_groups" do
    test "returns kqyrock api version request", %{state: state} do
      assert %Kayrock.DescribeGroups.V1.Request{} ==
               RequestTemplates.request_template(:describe_groups, state)
    end

    test "returns default api version request", %{state: state} do
      updated_state = Map.replace(state, :api_versions, %{})

      assert %Kayrock.DescribeGroups.V1.Request{} ==
               RequestTemplates.request_template(
                 :describe_groups,
                 updated_state
               )
    end
  end

  describe "request_template/2 - list offsets" do
    test "returns kayrock api version request", %{state: state} do
      assert %Kayrock.ListOffsets.V2.Request{} ==
               RequestTemplates.request_template(:list_offsets, state)
    end

    test "returns default api version request", %{state: state} do
      updated_state = Map.replace(state, :api_versions, %{})

      assert %Kayrock.ListOffsets.V1.Request{} ==
               RequestTemplates.request_template(:list_offsets, updated_state)
    end
  end
end
