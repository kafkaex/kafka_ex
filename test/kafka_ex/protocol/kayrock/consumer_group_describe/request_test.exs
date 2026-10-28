defmodule KafkaEx.Protocol.Kayrock.ConsumerGroupDescribe.RequestTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.ConsumerGroupDescribe

  for version <- [0, 1] do
    describe "V#{version} Request" do
      @version version

      test "sets group_ids and defaults include_authorized_operations to false" do
        request =
          @version
          |> Kayrock.ConsumerGroupDescribe.get_request_struct()
          |> ConsumerGroupDescribe.Request.build_request(group_ids: ["g1", "g2"])

        assert request.group_ids == ["g1", "g2"]
        assert request.include_authorized_operations == false

        wire = Kayrock.Request.serialize(%{request | correlation_id: 1, client_id: "c"})
        assert is_binary(IO.iodata_to_binary(wire))
      end

      test "include_authorized_operations is passed through" do
        request =
          @version
          |> Kayrock.ConsumerGroupDescribe.get_request_struct()
          |> ConsumerGroupDescribe.Request.build_request(group_ids: ["g"], include_authorized_operations: true)

        assert request.include_authorized_operations == true
      end

      test "requires group_ids" do
        assert_raise KeyError, fn ->
          @version
          |> Kayrock.ConsumerGroupDescribe.get_request_struct()
          |> ConsumerGroupDescribe.Request.build_request([])
        end
      end
    end
  end

  test "Any fallback builds from a plain map" do
    template = Map.from_struct(Kayrock.ConsumerGroupDescribe.V1.Request.__struct__())
    assert %{group_ids: ["g"]} = ConsumerGroupDescribe.Request.build_request(template, group_ids: ["g"])
  end
end
