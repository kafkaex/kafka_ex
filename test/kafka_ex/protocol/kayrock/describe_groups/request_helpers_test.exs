defmodule KafkaEx.Protocol.Kayrock.DescribeGroups.RequestHelpersTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.DescribeGroups.RequestHelpers

  describe "build_request_from_template/2" do
    test "sets groups from group_names option" do
      template = %{groups: [], correlation_id: nil, client_id: nil}
      result = RequestHelpers.build_request_from_template(template, group_names: ["g1", "g2"])

      assert result.groups == ["g1", "g2"]
    end

    test "handles single group" do
      template = %{groups: []}
      result = RequestHelpers.build_request_from_template(template, group_names: ["single"])

      assert result.groups == ["single"]
    end

    test "handles empty group list" do
      template = %{groups: []}
      result = RequestHelpers.build_request_from_template(template, group_names: [])

      assert result.groups == []
    end

    test "raises KeyError when group_names is missing" do
      template = %{groups: []}

      assert_raise KeyError, fn ->
        RequestHelpers.build_request_from_template(template, [])
      end
    end

    test "preserves other template fields" do
      template = %{groups: [], correlation_id: 42, client_id: "test-client"}
      result = RequestHelpers.build_request_from_template(template, group_names: ["g1"])

      assert result.correlation_id == 42
      assert result.client_id == "test-client"
    end

    test "overwrites existing groups" do
      template = %{groups: ["old-group"]}
      result = RequestHelpers.build_request_from_template(template, group_names: ["new-group"])

      assert result.groups == ["new-group"]
    end
  end

  describe "build_v3_plus_request/2" do
    test "sets groups and include_authorized_operations" do
      template = %{groups: [], include_authorized_operations: nil}

      result =
        RequestHelpers.build_v3_plus_request(template,
          group_names: ["g1"],
          include_authorized_operations: true
        )

      assert result.groups == ["g1"]
      assert result.include_authorized_operations == true
    end

    test "defaults include_authorized_operations to false" do
      template = %{groups: [], include_authorized_operations: nil}

      result = RequestHelpers.build_v3_plus_request(template, group_names: ["g1"])

      assert result.include_authorized_operations == false
    end

    test "sets include_authorized_operations to false explicitly" do
      template = %{groups: [], include_authorized_operations: nil}

      result =
        RequestHelpers.build_v3_plus_request(template,
          group_names: ["g1"],
          include_authorized_operations: false
        )

      assert result.include_authorized_operations == false
    end

    test "preserves other template fields" do
      template = %{
        groups: [],
        include_authorized_operations: nil,
        correlation_id: 77,
        client_id: "v3-client"
      }

      result = RequestHelpers.build_v3_plus_request(template, group_names: ["g1"])

      assert result.correlation_id == 77
      assert result.client_id == "v3-client"
    end

    test "raises KeyError when group_names is missing" do
      template = %{groups: [], include_authorized_operations: nil}

      assert_raise KeyError, fn ->
        RequestHelpers.build_v3_plus_request(template, [])
      end
    end
  end
end
