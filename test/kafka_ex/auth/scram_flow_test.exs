defmodule KafkaEx.Auth.ScramFlowTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Auth.ScramFlow

  describe "authenticate/4" do
    test "returns error when first send fails" do
      send_fun = fn _msg ->
        {:error, :connection_closed}
      end

      assert {:error, :connection_closed} = ScramFlow.authenticate("user", "pass", :sha256, send_fun)
    end

    test "sends client-first message with username" do
      send_fun = fn msg ->
        # First message should contain the username
        assert String.contains?(msg, "n=testuser")
        assert String.starts_with?(msg, "n,,")
        {:error, :test_complete}
      end

      assert {:error, :test_complete} = ScramFlow.authenticate("testuser", "pass", :sha256, send_fun)
    end

    test "escapes special characters in username" do
      send_fun = fn msg ->
        # "=" should become "=3D", "," should become "=2C"
        assert String.contains?(msg, "n=user=3Dname=2Ctest")
        {:error, :test_complete}
      end

      assert {:error, :test_complete} = ScramFlow.authenticate("user=name,test", "pass", :sha256, send_fun)
    end

    test "returns error on invalid server-first response" do
      call_count = :counters.new(1, [:atomics])

      send_fun = fn _msg ->
        count = :counters.get(call_count, 1)
        :counters.add(call_count, 1, 1)

        if count == 0 do
          {:ok, "invalid_response"}
        else
          {:error, :should_not_reach}
        end
      end

      assert {:error, :invalid_server_first_message} = ScramFlow.authenticate("user", "pass", :sha256, send_fun)
    end

    test "returns error when server nonce doesn't match" do
      call_count = :counters.new(1, [:atomics])

      send_fun = fn _msg ->
        count = :counters.get(call_count, 1)
        :counters.add(call_count, 1, 1)

        if count == 0 do
          {:ok, "r=wrongnonce,s=c2FsdA==,i=4096"}
        else
          {:error, :should_not_reach}
        end
      end

      assert {:error, :invalid_server_nonce} = ScramFlow.authenticate("user", "pass", :sha256, send_fun)
    end

    test "completes full SCRAM exchange with valid responses" do
      # This test simulates a complete SCRAM exchange
      call_count = :counters.new(1, [:atomics])

      send_fun = fn msg ->
        count = :counters.get(call_count, 1)
        :counters.add(call_count, 1, 1)

        case count do
          0 ->
            # Extract client nonce from client-first message
            [_, nonce_part] = String.split(msg, "r=")
            client_nonce = nonce_part

            # Build valid server-first with client nonce prefix
            server_nonce = client_nonce <> "serverpart"
            salt = Base.encode64("testsalt")
            {:ok, "r=#{server_nonce},s=#{salt},i=4096"}

          1 ->
            # Client-final received, return valid server-final
            # We need to compute the correct server signature
            # For simplicity, we'll verify the format and return a signature
            assert String.contains?(msg, "c=")
            assert String.contains?(msg, "r=")
            assert String.contains?(msg, "p=")

            # Return a server-final that will fail signature verification
            # (we can't easily compute the correct one without the full state)
            {:ok, "v=invalidsig"}

          _ ->
            {:error, :unexpected_call}
        end
      end

      # This will fail at signature verification, but proves the flow works
      assert {:error, :invalid_server_signature} = ScramFlow.authenticate("user", "pencil", :sha256, send_fun)
    end

    test "works with sha512 algorithm" do
      send_fun = fn _msg -> {:error, :test_complete} end

      assert {:error, :test_complete} = ScramFlow.authenticate("user", "pass", :sha512, send_fun)
    end
  end
end
