defmodule KafkaEx.Protocol.Kayrock.ApiVersions.RequestTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.ApiVersions

  describe "V0 request building" do
    test "builds V0 request with no parameters" do
      request = %Kayrock.ApiVersions.V0.Request{}

      result = ApiVersions.Request.build_request(request, [])

      assert %Kayrock.ApiVersions.V0.Request{} = result
      assert result == request
    end

    test "ignores any options passed" do
      request = %Kayrock.ApiVersions.V0.Request{correlation_id: 10, client_id: "test"}

      result = ApiVersions.Request.build_request(request, some_opt: "value", another: 42)

      assert result == request
      assert result.correlation_id == 10
      assert result.client_id == "test"
    end
  end

  describe "V1 request building" do
    test "builds V1 request with no parameters" do
      request = %Kayrock.ApiVersions.V1.Request{}

      result = ApiVersions.Request.build_request(request, [])

      assert %Kayrock.ApiVersions.V1.Request{} = result
      assert result == request
    end

    test "ignores any options passed" do
      request = %Kayrock.ApiVersions.V1.Request{correlation_id: 20, client_id: "test_v1"}

      result = ApiVersions.Request.build_request(request, foo: "bar", baz: 99)

      assert result == request
      assert result.correlation_id == 20
      assert result.client_id == "test_v1"
    end
  end

  describe "V2 request building" do
    test "builds V2 request with no parameters" do
      request = %Kayrock.ApiVersions.V2.Request{}

      result = ApiVersions.Request.build_request(request, [])

      assert %Kayrock.ApiVersions.V2.Request{} = result
      assert result == request
    end

    test "V2 request struct has same fields as V1" do
      v1 = %Kayrock.ApiVersions.V1.Request{}
      v2 = %Kayrock.ApiVersions.V2.Request{}

      assert Map.keys(v1) -- [:__struct__] == Map.keys(v2) -- [:__struct__]
    end

    test "ignores any options passed" do
      request = %Kayrock.ApiVersions.V2.Request{correlation_id: 42, client_id: "test"}

      result = ApiVersions.Request.build_request(request, some_opt: "value")

      assert result == request
      assert result.correlation_id == 42
      assert result.client_id == "test"
    end
  end

  describe "V3 request building" do
    test "builds V3 request with default client software fields" do
      request = %Kayrock.ApiVersions.V3.Request{}

      result = ApiVersions.Request.build_request(request, [])

      assert %Kayrock.ApiVersions.V3.Request{} = result
      assert result.client_software_name == "kafka_ex"
      assert result.client_software_version == Mix.Project.config()[:version]
    end

    test "allows overriding client_software_name" do
      request = %Kayrock.ApiVersions.V3.Request{}

      result = ApiVersions.Request.build_request(request, client_software_name: "my_app")

      assert result.client_software_name == "my_app"
    end

    test "allows overriding client_software_version" do
      request = %Kayrock.ApiVersions.V3.Request{}

      result = ApiVersions.Request.build_request(request, client_software_version: "2.5.0")

      assert result.client_software_version == "2.5.0"
    end

    test "allows overriding both client software fields" do
      request = %Kayrock.ApiVersions.V3.Request{}

      result =
        ApiVersions.Request.build_request(request,
          client_software_name: "custom_client",
          client_software_version: "3.0.0"
        )

      assert result.client_software_name == "custom_client"
      assert result.client_software_version == "3.0.0"
    end

    test "preserves correlation_id and client_id" do
      request = %Kayrock.ApiVersions.V3.Request{correlation_id: 99, client_id: "test_client"}

      result = ApiVersions.Request.build_request(request, [])

      assert result.correlation_id == 99
      assert result.client_id == "test_client"
    end

    test "preserves tagged_fields" do
      request = %Kayrock.ApiVersions.V3.Request{tagged_fields: [{0, <<1, 2, 3>>}]}

      result = ApiVersions.Request.build_request(request, [])

      assert result.tagged_fields == [{0, <<1, 2, 3>>}]
    end

    test "V3 request struct has client_software fields not present in V2" do
      v2 = %Kayrock.ApiVersions.V2.Request{}
      v3 = %Kayrock.ApiVersions.V3.Request{}

      v2_keys = Map.keys(v2) -- [:__struct__]
      v3_keys = Map.keys(v3) -- [:__struct__]

      new_fields = v3_keys -- v2_keys
      assert :client_software_name in new_fields
      assert :client_software_version in new_fields
      assert :tagged_fields in new_fields
    end
  end

  describe "V3 request building - edge cases" do
    test "sets empty string client_software_name when overridden" do
      request = %Kayrock.ApiVersions.V3.Request{}

      result = ApiVersions.Request.build_request(request, client_software_name: "")

      assert result.client_software_name == ""
      # client_software_version still gets default
      assert result.client_software_version == Mix.Project.config()[:version]
    end

    test "sets empty string client_software_version when overridden" do
      request = %Kayrock.ApiVersions.V3.Request{}

      result = ApiVersions.Request.build_request(request, client_software_version: "")

      assert result.client_software_version == ""
    end

    test "ignores unrelated options while applying client software fields" do
      request = %Kayrock.ApiVersions.V3.Request{}

      result =
        ApiVersions.Request.build_request(request,
          client_software_name: "test_app",
          unrelated_key: "ignored",
          another: 42
        )

      assert result.client_software_name == "test_app"
      assert result.client_software_version == Mix.Project.config()[:version]
    end

    test "preserves existing tagged_fields when building request" do
      tagged = [{0, <<1, 2, 3>>}, {1, <<4, 5>>}]
      request = %Kayrock.ApiVersions.V3.Request{tagged_fields: tagged}

      result =
        ApiVersions.Request.build_request(request,
          client_software_name: "app",
          client_software_version: "1.0.0"
        )

      assert result.tagged_fields == tagged
      assert result.client_software_name == "app"
    end

    test "overwrites pre-set client_software fields" do
      request = %Kayrock.ApiVersions.V3.Request{
        client_software_name: "old_name",
        client_software_version: "0.1.0"
      }

      result =
        ApiVersions.Request.build_request(request,
          client_software_name: "new_name",
          client_software_version: "2.0.0"
        )

      assert result.client_software_name == "new_name"
      assert result.client_software_version == "2.0.0"
    end
  end

  describe "Any fallback request implementation (forward compatibility)" do
    # The @fallback_to_any true on the Request protocol means any struct type
    # without an explicit implementation gets the Any fallback, which returns
    # the request unchanged. This simulates a hypothetical future V4+ struct.

    defmodule FakeV4Request do
      defstruct [:correlation_id, :client_id, :some_new_field]
    end

    test "returns unknown struct unchanged via Any fallback" do
      request = %FakeV4Request{
        correlation_id: 123,
        client_id: "test",
        some_new_field: "hello"
      }

      result = ApiVersions.Request.build_request(request, [])

      assert result == request
      assert result.correlation_id == 123
      assert result.client_id == "test"
      assert result.some_new_field == "hello"
    end

    test "Any fallback ignores options" do
      request = %FakeV4Request{correlation_id: 1, client_id: "c"}

      result =
        ApiVersions.Request.build_request(request,
          client_software_name: "should_be_ignored",
          client_software_version: "1.0.0"
        )

      # The Any implementation does not process any options
      assert result == request
    end

    test "Any fallback works with a plain map" do
      # Maps also implement the Any protocol
      request = %{correlation_id: 42, client_id: "test"}

      result = ApiVersions.Request.build_request(request, [])

      assert result == request
    end
  end

  describe "build_request via KayrockProtocol" do
    alias KafkaEx.Protocol.KayrockProtocol

    test "build_request dispatches to V0" do
      result = KayrockProtocol.build_request(:api_versions, 0, [])

      assert %Kayrock.ApiVersions.V0.Request{} = result
    end

    test "build_request dispatches to V1" do
      result = KayrockProtocol.build_request(:api_versions, 1, [])

      assert %Kayrock.ApiVersions.V1.Request{} = result
    end

    test "build_request dispatches to V2" do
      result = KayrockProtocol.build_request(:api_versions, 2, [])

      assert %Kayrock.ApiVersions.V2.Request{} = result
    end

    test "build_request dispatches to V3 with defaults" do
      result = KayrockProtocol.build_request(:api_versions, 3, [])

      assert %Kayrock.ApiVersions.V3.Request{} = result
      assert result.client_software_name == "kafka_ex"
    end

    test "build_request dispatches to V3 with custom client software" do
      result =
        KayrockProtocol.build_request(:api_versions, 3,
          client_software_name: "custom",
          client_software_version: "1.0.0"
        )

      assert %Kayrock.ApiVersions.V3.Request{} = result
      assert result.client_software_name == "custom"
      assert result.client_software_version == "1.0.0"
    end
  end
end
