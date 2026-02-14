defmodule KafkaEx.Protocol.Kayrock.Heartbeat.ResponseTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Protocol.Kayrock.Heartbeat
  alias KafkaEx.Messages.Heartbeat, as: HeartbeatStruct

  # ---- V0 Response ----

  describe "V0 Response implementation" do
    test "parses successful response with no error" do
      response = %Kayrock.Heartbeat.V0.Response{
        error_code: 0
      }

      assert {:ok, :no_error} = Heartbeat.Response.parse_response(response)
    end

    test "parses error response with rebalance_in_progress" do
      response = %Kayrock.Heartbeat.V0.Response{
        error_code: 27
      }

      assert {:error, error} = Heartbeat.Response.parse_response(response)
      assert error.error == :rebalance_in_progress
      assert error.metadata == %{}
    end

    test "parses error response with unknown_member_id" do
      response = %Kayrock.Heartbeat.V0.Response{
        error_code: 25
      }

      assert {:error, error} = Heartbeat.Response.parse_response(response)
      assert error.error == :unknown_member_id
    end

    test "parses error response with illegal_generation" do
      response = %Kayrock.Heartbeat.V0.Response{
        error_code: 22
      }

      assert {:error, error} = Heartbeat.Response.parse_response(response)
      assert error.error == :illegal_generation
    end

    test "parses error response with coordinator_not_available" do
      response = %Kayrock.Heartbeat.V0.Response{
        error_code: 15
      }

      assert {:error, error} = Heartbeat.Response.parse_response(response)
      assert error.error == :coordinator_not_available
    end

    test "parses error response with not_coordinator" do
      response = %Kayrock.Heartbeat.V0.Response{
        error_code: 16
      }

      assert {:error, error} = Heartbeat.Response.parse_response(response)
      assert error.error == :not_coordinator
    end

    test "parses error response with group_authorization_failed" do
      response = %Kayrock.Heartbeat.V0.Response{
        error_code: 30
      }

      assert {:error, error} = Heartbeat.Response.parse_response(response)
      assert error.error == :group_authorization_failed
    end

    test "error struct has empty metadata" do
      response = %Kayrock.Heartbeat.V0.Response{
        error_code: 27
      }

      {:error, error} = Heartbeat.Response.parse_response(response)

      assert error.metadata == %{}
    end
  end

  # ---- V1 Response ----

  describe "V1 Response implementation" do
    test "parses successful response with throttle_time_ms" do
      response = %Kayrock.Heartbeat.V1.Response{
        error_code: 0,
        throttle_time_ms: 0
      }

      assert {:ok, heartbeat} = Heartbeat.Response.parse_response(response)
      assert %HeartbeatStruct{throttle_time_ms: 0} = heartbeat
    end

    test "parses successful response with non-zero throttle time" do
      response = %Kayrock.Heartbeat.V1.Response{
        error_code: 0,
        throttle_time_ms: 100
      }

      assert {:ok, heartbeat} = Heartbeat.Response.parse_response(response)
      assert heartbeat.throttle_time_ms == 100
    end

    test "parses error response with rebalance_in_progress" do
      response = %Kayrock.Heartbeat.V1.Response{
        error_code: 27
      }

      assert {:error, error} = Heartbeat.Response.parse_response(response)
      assert error.error == :rebalance_in_progress
    end

    test "parses error response with unknown_member_id" do
      response = %Kayrock.Heartbeat.V1.Response{
        error_code: 25
      }

      assert {:error, error} = Heartbeat.Response.parse_response(response)
      assert error.error == :unknown_member_id
    end

    test "parses error response with illegal_generation" do
      response = %Kayrock.Heartbeat.V1.Response{
        error_code: 22
      }

      assert {:error, error} = Heartbeat.Response.parse_response(response)
      assert error.error == :illegal_generation
    end
  end

  # ---- V2 Response ----

  describe "V2 Response implementation" do
    test "parses successful response with throttle_time_ms" do
      response = %Kayrock.Heartbeat.V2.Response{
        error_code: 0,
        throttle_time_ms: 200
      }

      assert {:ok, heartbeat} = Heartbeat.Response.parse_response(response)
      assert %HeartbeatStruct{throttle_time_ms: 200} = heartbeat
    end

    test "parses successful response with zero throttle time" do
      response = %Kayrock.Heartbeat.V2.Response{
        error_code: 0,
        throttle_time_ms: 0
      }

      assert {:ok, heartbeat} = Heartbeat.Response.parse_response(response)
      assert heartbeat.throttle_time_ms == 0
    end

    test "parses error response with unknown_member_id" do
      response = %Kayrock.Heartbeat.V2.Response{
        error_code: 25,
        throttle_time_ms: 10
      }

      assert {:error, error} = Heartbeat.Response.parse_response(response)
      assert error.error == :unknown_member_id
    end

    test "parses error response with rebalance_in_progress" do
      response = %Kayrock.Heartbeat.V2.Response{
        error_code: 27,
        throttle_time_ms: 0
      }

      assert {:error, error} = Heartbeat.Response.parse_response(response)
      assert error.error == :rebalance_in_progress
    end
  end

  # ---- V3 Response ----

  describe "V3 Response implementation" do
    test "parses successful response with throttle_time_ms" do
      response = %Kayrock.Heartbeat.V3.Response{
        error_code: 0,
        throttle_time_ms: 150
      }

      assert {:ok, heartbeat} = Heartbeat.Response.parse_response(response)
      assert %HeartbeatStruct{throttle_time_ms: 150} = heartbeat
    end

    test "parses error response" do
      response = %Kayrock.Heartbeat.V3.Response{
        error_code: 25,
        throttle_time_ms: 0
      }

      assert {:error, error} = Heartbeat.Response.parse_response(response)
      assert error.error == :unknown_member_id
    end

    test "parses successful response with zero throttle time" do
      response = %Kayrock.Heartbeat.V3.Response{
        error_code: 0,
        throttle_time_ms: 0
      }

      assert {:ok, heartbeat} = Heartbeat.Response.parse_response(response)
      assert heartbeat.throttle_time_ms == 0
    end
  end

  # ---- V4 Response (FLEX) ----

  describe "V4 Response implementation (FLEX)" do
    test "parses successful response (compact encoding handled by Kayrock)" do
      response = %Kayrock.Heartbeat.V4.Response{
        error_code: 0,
        throttle_time_ms: 300,
        tagged_fields: []
      }

      assert {:ok, heartbeat} = Heartbeat.Response.parse_response(response)
      assert %HeartbeatStruct{throttle_time_ms: 300} = heartbeat
    end

    test "parses error response" do
      response = %Kayrock.Heartbeat.V4.Response{
        error_code: 25,
        throttle_time_ms: 0,
        tagged_fields: []
      }

      assert {:error, error} = Heartbeat.Response.parse_response(response)
      assert error.error == :unknown_member_id
    end

    test "preserves throttle_time_ms with tagged_fields" do
      response = %Kayrock.Heartbeat.V4.Response{
        error_code: 0,
        throttle_time_ms: 500,
        tagged_fields: [{0, <<1, 2, 3>>}]
      }

      assert {:ok, heartbeat} = Heartbeat.Response.parse_response(response)
      assert heartbeat.throttle_time_ms == 500
    end

    test "parses successful response with zero throttle time" do
      response = %Kayrock.Heartbeat.V4.Response{
        error_code: 0,
        throttle_time_ms: 0,
        tagged_fields: []
      }

      assert {:ok, heartbeat} = Heartbeat.Response.parse_response(response)
      assert heartbeat.throttle_time_ms == 0
    end
  end

  # ---- Cross-version consistency ----

  describe "Cross-version response consistency (V1-V4)" do
    @v1_v4_response_structs [
      {struct(Kayrock.Heartbeat.V1.Response, %{
         throttle_time_ms: 100,
         error_code: 0
       }), "V1"},
      {struct(Kayrock.Heartbeat.V2.Response, %{
         throttle_time_ms: 100,
         error_code: 0
       }), "V2"},
      {struct(Kayrock.Heartbeat.V3.Response, %{
         throttle_time_ms: 100,
         error_code: 0
       }), "V3"},
      {struct(Kayrock.Heartbeat.V4.Response, %{
         throttle_time_ms: 100,
         error_code: 0,
         tagged_fields: []
       }), "V4"}
    ]

    test "V1-V4 produce identical domain results" do
      results =
        Enum.map(@v1_v4_response_structs, fn {response, label} ->
          {:ok, result} = Heartbeat.Response.parse_response(response)
          {label, result}
        end)

      for {label, result} <- results do
        assert %HeartbeatStruct{} = result,
               "#{label}: expected Heartbeat struct"

        assert result.throttle_time_ms == 100,
               "#{label}: expected throttle_time_ms 100, got #{result.throttle_time_ms}"
      end
    end

    test "V0 returns :no_error while V1-V4 return Heartbeat struct" do
      v0_response = %Kayrock.Heartbeat.V0.Response{error_code: 0}

      v1_response = %Kayrock.Heartbeat.V1.Response{
        error_code: 0,
        throttle_time_ms: 50
      }

      v0_result = Heartbeat.Response.parse_response(v0_response)
      v1_result = Heartbeat.Response.parse_response(v1_response)

      assert v0_result == {:ok, :no_error}
      assert {:ok, %HeartbeatStruct{throttle_time_ms: 50}} = v1_result
    end

    test "error responses are identical between all versions" do
      v0_response = %Kayrock.Heartbeat.V0.Response{error_code: 27}
      v1_response = %Kayrock.Heartbeat.V1.Response{error_code: 27, throttle_time_ms: 0}
      v2_response = %Kayrock.Heartbeat.V2.Response{error_code: 27, throttle_time_ms: 0}
      v3_response = %Kayrock.Heartbeat.V3.Response{error_code: 27, throttle_time_ms: 0}

      v4_response = %Kayrock.Heartbeat.V4.Response{
        error_code: 27,
        throttle_time_ms: 0,
        tagged_fields: []
      }

      errors =
        Enum.map(
          [v0_response, v1_response, v2_response, v3_response, v4_response],
          fn resp ->
            {:error, error} = Heartbeat.Response.parse_response(resp)
            error
          end
        )

      for error <- errors do
        assert error.error == :rebalance_in_progress
        assert error.metadata == %{}
      end
    end
  end

  # ---- Any fallback ----

  describe "Any fallback Response implementation" do
    test "routes V1+-like map to throttle_time path" do
      response = %{
        throttle_time_ms: 250,
        error_code: 0
      }

      assert {:ok, heartbeat} = Heartbeat.Response.parse_response(response)
      assert %HeartbeatStruct{throttle_time_ms: 250} = heartbeat
    end

    test "routes V0-like map to :no_error path" do
      response = %{
        error_code: 0
      }

      assert {:ok, :no_error} = Heartbeat.Response.parse_response(response)
    end

    test "handles error response via V1+ Any path" do
      response = %{
        throttle_time_ms: 0,
        error_code: 22
      }

      assert {:error, error} = Heartbeat.Response.parse_response(response)
      assert error.error == :illegal_generation
    end

    test "handles error response via V0 Any path" do
      response = %{
        error_code: 27
      }

      assert {:error, error} = Heartbeat.Response.parse_response(response)
      assert error.error == :rebalance_in_progress
    end

    test "V1+ path returns Heartbeat struct with throttle_time_ms" do
      response = %{
        throttle_time_ms: 500,
        error_code: 0
      }

      assert {:ok, heartbeat} = Heartbeat.Response.parse_response(response)
      assert heartbeat.throttle_time_ms == 500
    end

    test "V0 path returns :no_error atom (not Heartbeat struct)" do
      response = %{error_code: 0}

      result = Heartbeat.Response.parse_response(response)

      assert result == {:ok, :no_error}
      refute match?({:ok, %HeartbeatStruct{}}, result)
    end

    test "handles zero throttle_time_ms via Any path" do
      response = %{
        throttle_time_ms: 0,
        error_code: 0
      }

      assert {:ok, heartbeat} = Heartbeat.Response.parse_response(response)
      assert heartbeat.throttle_time_ms == 0
    end
  end
end
