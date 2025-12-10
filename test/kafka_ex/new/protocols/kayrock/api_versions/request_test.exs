defmodule KafkaEx.New.Protocols.Kayrock.ApiVersions.RequestTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.ApiVersions

  describe "V0 request building" do
    test "builds V0 request with no parameters" do
      request = %Kayrock.ApiVersions.V0.Request{}

      result = ApiVersions.Request.build_request(request, [])

      assert %Kayrock.ApiVersions.V0.Request{} = result
      assert result == request
    end
  end

  describe "V1 request building" do
    test "builds V1 request with no parameters" do
      request = %Kayrock.ApiVersions.V1.Request{}

      result = ApiVersions.Request.build_request(request, [])

      assert %Kayrock.ApiVersions.V1.Request{} = result
      assert result == request
    end
  end
end
