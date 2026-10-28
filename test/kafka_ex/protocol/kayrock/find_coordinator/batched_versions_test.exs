defmodule KafkaEx.Protocol.Kayrock.FindCoordinator.BatchedVersionsTest do
  @moduledoc "FindCoordinator V4-V6 (KIP-699 coordinator_keys / coordinators)."
  use ExUnit.Case, async: true

  alias KafkaEx.Client.Error
  alias KafkaEx.Cluster.Broker
  alias KafkaEx.Messages.FindCoordinator
  alias KafkaEx.Protocol.Kayrock.FindCoordinator.Request, as: FindCoordinatorRequest
  alias KafkaEx.Protocol.Kayrock.FindCoordinator.Response, as: FindCoordinatorResponse
  alias KafkaEx.Protocol.KayrockProtocol

  @versions [Kayrock.FindCoordinator.V4, Kayrock.FindCoordinator.V5, Kayrock.FindCoordinator.V6]

  defp serialize!(request) do
    request
    |> Map.merge(%{correlation_id: 1, client_id: "test"})
    |> Kayrock.Request.serialize()
    |> IO.iodata_to_binary()
  end

  for mod <- @versions do
    describe "#{inspect(mod)}" do
      test "request sends the group id as the single coordinator key" do
        request = FindCoordinatorRequest.build_request(struct(unquote(mod).Request), group_id: "g")
        assert %{coordinator_keys: ["g"], key_type: 0} = request
        assert is_binary(serialize!(request))
      end

      test "request honours coordinator_key / coordinator_type" do
        request =
          FindCoordinatorRequest.build_request(struct(unquote(mod).Request),
            coordinator_key: "txn-1",
            coordinator_type: :transaction
          )

        assert %{coordinator_keys: ["txn-1"], key_type: 1} = request
      end

      test "response parses the single coordinator" do
        response =
          struct(unquote(mod).Response, %{
            throttle_time_ms: 7,
            coordinators: [
              %{key: "g", node_id: 2, host: "b2", port: 9093, error_code: 0, error_message: nil, tagged_fields: []}
            ],
            tagged_fields: []
          })

        assert {:ok,
                %FindCoordinator{
                  coordinator: %Broker{node_id: 2, host: "b2", port: 9093},
                  error_code: :no_error,
                  throttle_time_ms: 7,
                  error_message: nil
                } = result} = FindCoordinatorResponse.parse_response(response)

        assert FindCoordinator.coordinator_node_id(result) == 2
      end

      test "response surfaces the per-coordinator error" do
        response =
          struct(unquote(mod).Response, %{
            throttle_time_ms: 0,
            coordinators: [
              %{key: "g", node_id: -1, host: "", port: -1, error_code: 15, error_message: "not available"}
            ]
          })

        assert {:error, %Error{error: :coordinator_not_available}} = FindCoordinatorResponse.parse_response(response)
      end

      test "empty coordinators is an error" do
        response = struct(unquote(mod).Response, %{throttle_time_ms: 0, coordinators: []})
        assert {:error, %Error{error: :coordinator_not_available}} = FindCoordinatorResponse.parse_response(response)
      end
    end
  end

  test "same result as V3 for the same coordinator" do
    v3 = %Kayrock.FindCoordinator.V3.Response{
      throttle_time_ms: 0,
      error_code: 0,
      error_message: nil,
      node_id: 1,
      host: "h",
      port: 9092
    }

    v6 = %Kayrock.FindCoordinator.V6.Response{
      throttle_time_ms: 0,
      coordinators: [%{key: "g", node_id: 1, host: "h", port: 9092, error_code: 0, error_message: nil}]
    }

    assert FindCoordinatorResponse.parse_response(v3) == FindCoordinatorResponse.parse_response(v6)
  end

  test "KayrockProtocol dispatches V4-V6" do
    for v <- 4..6 do
      request = KayrockProtocol.build_request(:find_coordinator, v, group_id: "g")
      assert request.__struct__ == Module.concat([Kayrock.FindCoordinator, :"V#{v}", Request])
      assert request.coordinator_keys == ["g"]
    end
  end

  describe "Any fallback" do
    defmodule FakeBatchedRequest do
      defstruct [:key_type, :coordinator_keys]
    end

    defmodule FakeBatchedResponse do
      defstruct [:throttle_time_ms, :coordinators]
    end

    test "batched request / response shapes" do
      assert %{coordinator_keys: ["g"], key_type: 0} =
               FindCoordinatorRequest.build_request(%FakeBatchedRequest{}, group_id: "g")

      response = %FakeBatchedResponse{
        throttle_time_ms: 0,
        coordinators: [%{key: "g", node_id: 3, host: "h", port: 1, error_code: 0, error_message: nil}]
      }

      assert {:ok, %FindCoordinator{coordinator: %Broker{node_id: 3}}} =
               FindCoordinatorResponse.parse_response(response)
    end
  end
end
