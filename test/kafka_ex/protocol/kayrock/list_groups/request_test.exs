defmodule KafkaEx.Protocol.Kayrock.ListGroups.RequestTest do
  use ExUnit.Case, async: true

  # ListGroups has no request fields at any version, so build_request/2 is a
  # pass-through: it returns the template struct unchanged.

  alias KafkaEx.Protocol.Kayrock.ListGroups

  describe "V0-V2 Request implementations (no request fields)" do
    test "V0 returns the request struct unchanged" do
      request = %Kayrock.ListGroups.V0.Request{correlation_id: 1, client_id: "c"}
      assert ListGroups.Request.build_request(request, []) == request
    end

    test "V1 returns the request struct unchanged" do
      request = %Kayrock.ListGroups.V1.Request{correlation_id: 2, client_id: "c"}
      assert ListGroups.Request.build_request(request, []) == request
    end

    test "V2 returns the request struct unchanged" do
      request = %Kayrock.ListGroups.V2.Request{correlation_id: 3, client_id: "c"}
      assert ListGroups.Request.build_request(request, []) == request
    end
  end

  describe "V3 Request implementation (FLEX)" do
    test "returns the request struct unchanged and keeps tagged_fields default" do
      request = %Kayrock.ListGroups.V3.Request{correlation_id: 4, client_id: "c"}

      result = ListGroups.Request.build_request(request, [])

      assert result == request
      assert result.tagged_fields == []
    end
  end

  describe "Any fallback Request implementation" do
    test "returns an unknown-version template unchanged" do
      template = %{some_field: "future"}
      assert ListGroups.Request.build_request(template, []) == template
    end
  end
end
