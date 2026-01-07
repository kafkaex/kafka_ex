defmodule KafkaEx.Auth.SASL.CodecBinaryTest do
  use ExUnit.Case, async: true
  alias KafkaEx.Auth.SASL.CodecBinary

  describe "api_versions_request/3" do
    test "builds correct request with default client_id" do
      corr = 123
      ver = 0

      result = CodecBinary.api_versions_request(corr, ver)

      <<api_key::16, api_ver::16, correlation::32, client_len::16, client_id::binary-size(client_len)>> = result

      assert api_key == 18
      assert api_ver == ver
      assert correlation == corr
      assert client_id == "kafka_ex"
    end

    test "builds correct request with custom client_id" do
      result = CodecBinary.api_versions_request(456, 1, "my_client")

      <<_api_key::16, _ver::16, _corr::32, client_len::16, client_id::binary-size(client_len)>> = result

      assert client_id == "my_client"
    end
  end

  describe "handshake_request/4" do
    test "builds correct handshake request" do
      mech = "SCRAM-SHA-256"
      corr = 789
      ver = 1

      result = CodecBinary.handshake_request(mech, corr, ver)

      <<api_key::16, api_ver::16, correlation::32, client_len::16, client_id::binary-size(client_len), mech_len::16,
        mechanism::binary-size(mech_len)>> = result

      assert api_key == 17
      assert api_ver == ver
      assert correlation == corr
      assert client_id == "kafka_ex"
      assert mechanism == mech
    end

    test "handles different mechanism names" do
      for mech <- ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"] do
        result = CodecBinary.handshake_request(mech, 1, 0)

        # Skip the header (api_key, version, correlation_id)
        <<_api_key::16, _ver::16, _corr::32, rest::binary>> = result
        # Parse client_id
        <<client_len::16, _client_id::binary-size(client_len), rest2::binary>> = rest
        # Parse mechanism
        <<mech_len::16, mechanism::binary-size(mech_len)>> = rest2

        assert mechanism == mech
      end
    end
  end

  describe "authenticate_request/4" do
    test "builds correct authenticate request" do
      auth_bytes = "auth_payload_data"
      corr = 321
      ver = 0

      result = CodecBinary.authenticate_request(auth_bytes, corr, ver)

      <<api_key::16, api_ver::16, correlation::32, client_len::16, _client_id::binary-size(client_len), auth_len::32,
        auth_data::binary-size(auth_len)>> = result

      assert api_key == 36
      assert api_ver == ver
      assert correlation == corr
      assert auth_data == auth_bytes
    end

    test "handles binary auth data correctly" do
      # Test with various binary patterns including null bytes
      auth_bytes = <<0, 1, 2, 3, 255, 254, 253>>
      result = CodecBinary.authenticate_request(auth_bytes, 1, 0)

      # Skip header
      <<_api_key::16, _ver::16, _corr::32, rest::binary>> = result
      # Skip client_id
      <<client_len::16, _client_id::binary-size(client_len), rest2::binary>> = rest
      # Get auth data
      <<auth_len::32, auth_data::binary-size(auth_len)>> = rest2

      assert auth_data == auth_bytes
    end
  end

  describe "parse_api_versions_response/2" do
    test "parses valid response with multiple API versions" do
      corr = 42
      # Build a response: corr_id, error_code, array_count, then api entries
      response = <<
        # correlation id
        corr::32,
        # error code (0 = success)
        0::16,
        # number of API entries
        3::32,
        # API key 1: min=0, max=2
        1::16,
        0::16,
        2::16,
        # API key 17 (handshake): min=0, max=1  
        17::16,
        0::16,
        1::16,
        # API key 36 (authenticate): min=0, max=2
        36::16,
        0::16,
        2::16
      >>

      result = CodecBinary.parse_api_versions_response(response, corr)

      assert result == %{
               1 => {0, 2},
               17 => {0, 1},
               36 => {0, 2}
             }
    end

    test "returns error on correlation mismatch" do
      response = <<42::32, 0::16, 0::32>>

      assert {:error, :correlation_mismatch} =
               CodecBinary.parse_api_versions_response(response, 99)
    end

    test "returns error for invalid/short response" do
      assert {:error, :incomplete_response} = CodecBinary.parse_api_versions_response(<<>>, 1)
      assert {:error, :incomplete_response} = CodecBinary.parse_api_versions_response(<<1, 2, 3>>, 1)
    end

    test "handles empty API list" do
      corr = 10
      response = <<corr::32, 0::16, 0::32>>

      assert %{} = CodecBinary.parse_api_versions_response(response, corr)
    end

    test "handles incomplete responses" do
      corr = 123

      # Very short response (< 4 bytes) returns incomplete error
      assert {:error, :incomplete_response} = CodecBinary.parse_api_versions_response(<<1, 2>>, corr)

      # Response with just correlation (4 bytes) - needs more data
      assert {:error, :incomplete_response} = CodecBinary.parse_api_versions_response(<<corr::32>>, corr)

      # Response with correlation + partial data (6 bytes) - still incomplete
      assert {:error, :incomplete_response} = CodecBinary.parse_api_versions_response(<<corr::32, 0::16>>, corr)

      # Complete response structure parses correctly (empty API list)
      response_complete = <<corr::32, 0::16, 0::32>>
      assert %{} = CodecBinary.parse_api_versions_response(response_complete, corr)

      # Wrong correlation returns specific error
      assert {:error, :correlation_mismatch} = CodecBinary.parse_api_versions_response(<<456::32, 0::16, 0::32>>, corr)
    end
  end

  describe "parse_handshake_response/4" do
    test "parses v0 success response" do
      corr = 123
      response = <<corr::32, 0::16>>

      assert :ok = CodecBinary.parse_handshake_response(response, corr, "PLAIN", 0)
    end

    test "parses v1 success with matching mechanism" do
      corr = 456
      mechanisms = ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"]

      # Build response with mechanism list
      mech_bytes = for m <- mechanisms, into: <<>>, do: <<byte_size(m)::16, m::binary>>
      response = <<corr::32, 0::16, length(mechanisms)::32, mech_bytes::binary>>

      assert :ok = CodecBinary.parse_handshake_response(response, corr, "SCRAM-SHA-256", 1)
    end

    test "returns error for unsupported mechanism in v1" do
      corr = 789
      mechanisms = ["PLAIN", "SCRAM-SHA-256"]

      mech_bytes = for m <- mechanisms, into: <<>>, do: <<byte_size(m)::16, m::binary>>
      response = <<corr::32, 0::16, length(mechanisms)::32, mech_bytes::binary>>

      assert {:error, {:unsupported_mechanism_on_broker, "SCRAM-SHA-512"}} =
               CodecBinary.parse_handshake_response(response, corr, "SCRAM-SHA-512", 1)
    end

    test "returns error on correlation mismatch" do
      response = <<123::32, 0::16>>

      assert {:error, :correlation_mismatch} =
               CodecBinary.parse_handshake_response(response, 456, "PLAIN", 0)
    end

    test "returns appropriate error for known error codes" do
      corr = 100

      # Test unsupported SASL mechanism error
      response = <<corr::32, 33::16>>

      assert {:error, {:handshake_failed, :unsupported_sasl_mechanism}} =
               CodecBinary.parse_handshake_response(response, corr, "PLAIN", 0)

      # Test illegal SASL state error
      response = <<corr::32, 34::16>>

      assert {:error, {:handshake_failed, :illegal_sasl_state}} =
               CodecBinary.parse_handshake_response(response, corr, "PLAIN", 0)
    end
  end

  describe "parse_authenticate_response/3" do
    test "parses v0 success with auth bytes" do
      corr = 111
      auth_data = "server_response"
      response = <<corr::32, 0::16, byte_size(auth_data)::32, auth_data::binary>>

      assert {:ok, ^auth_data} = CodecBinary.parse_authenticate_response(response, corr, 0)
    end

    test "parses v0 success with null auth bytes" do
      corr = 222
      response = <<corr::32, 0::16, -1::32-signed>>

      assert {:ok, nil} = CodecBinary.parse_authenticate_response(response, corr, 0)
    end

    test "parses v1+ success with error message and auth bytes" do
      corr = 333
      error_msg = "some_error"
      auth_data = "auth_response"

      response = <<
        corr::32,
        0::16,
        byte_size(error_msg)::16,
        error_msg::binary,
        byte_size(auth_data)::32,
        auth_data::binary
      >>

      assert {:ok, ^auth_data} = CodecBinary.parse_authenticate_response(response, corr, 1)
    end

    test "parses v1+ with null error message" do
      corr = 444
      auth_data = "response"

      response = <<
        corr::32,
        0::16,
        # null error message
        -1::16-signed,
        byte_size(auth_data)::32,
        auth_data::binary
      >>

      assert {:ok, ^auth_data} = CodecBinary.parse_authenticate_response(response, corr, 1)
    end

    test "returns error on correlation mismatch" do
      response = <<100::32, 0::16, 0::32>>

      assert {:error, :correlation_mismatch} =
               CodecBinary.parse_authenticate_response(response, 200, 0)
    end

    test "returns appropriate error for authentication failure" do
      corr = 555
      response = <<corr::32, 58::16, 0::32>>

      assert {:error, {:auth_failed, :sasl_authentication_failed}} =
               CodecBinary.parse_authenticate_response(response, corr, 0)
    end

    test "returns generic error for unknown error codes" do
      corr = 666
      response = <<corr::32, 999::16, 0::32>>

      assert {:error, {:auth_failed, {:error_code, 999}}} =
               CodecBinary.parse_authenticate_response(response, corr, 0)
    end
  end

  describe "kafka_error_name/1" do
    test "maps known SASL error codes to atoms" do
      assert :unsupported_sasl_mechanism = CodecBinary.kafka_error_name(33)
      assert :illegal_sasl_state = CodecBinary.kafka_error_name(34)
      assert :unsupported_version = CodecBinary.kafka_error_name(35)
      assert :sasl_authentication_failed = CodecBinary.kafka_error_name(58)
    end

    test "returns generic error tuple for unknown codes" do
      # 0 is actually "no_error" but we treat it as unknown for non-SASL errors
      assert {:error_code, 0} = CodecBinary.kafka_error_name(0)
      assert {:error_code, 1} = CodecBinary.kafka_error_name(1)
      assert {:error_code, 999} = CodecBinary.kafka_error_name(999)
    end
  end

  describe "pick_handshake_version/1" do
    test "picks highest supported version within broker range" do
      # Broker supports 0-2, we prefer [0, 1]
      api_versions = %{17 => {0, 2}}
      assert 1 = CodecBinary.pick_handshake_version(api_versions)
    end

    test "picks lower version when broker doesn't support higher" do
      # Broker only supports 0
      api_versions = %{17 => {0, 0}}
      assert 0 = CodecBinary.pick_handshake_version(api_versions)
    end

    test "returns 0 when API not listed (old broker)" do
      api_versions = %{1 => {0, 1}, 2 => {0, 1}}
      assert 0 = CodecBinary.pick_handshake_version(api_versions)
    end

    test "returns 0 for empty API versions" do
      assert 0 = CodecBinary.pick_handshake_version(%{})
    end
  end

  describe "pick_authenticate_version/1" do
    test "picks highest supported version within broker range" do
      # Broker supports 0-3, we prefer [0, 1, 2]
      api_versions = %{36 => {0, 3}}
      assert 2 = CodecBinary.pick_authenticate_version(api_versions)
    end

    test "picks exact match when only one version supported" do
      api_versions = %{36 => {1, 1}}
      assert 1 = CodecBinary.pick_authenticate_version(api_versions)
    end

    test "returns 0 when API not supported" do
      api_versions = %{}
      assert 0 = CodecBinary.pick_authenticate_version(api_versions)
    end
  end

  describe "pick_supported_version/3" do
    test "picks highest preferred version in broker range" do
      api_versions = %{10 => {2, 5}}
      preferred = [1, 2, 3, 4, 5, 6]

      assert 5 = CodecBinary.pick_supported_version(api_versions, 10, preferred)
    end

    test "returns 0 when no overlap between broker and preferred" do
      api_versions = %{10 => {5, 7}}
      preferred = [0, 1, 2, 3]

      assert 0 = CodecBinary.pick_supported_version(api_versions, 10, preferred)
    end

    test "handles single version preference" do
      api_versions = %{10 => {0, 10}}
      preferred = [3]

      assert 3 = CodecBinary.pick_supported_version(api_versions, 10, preferred)
    end

    test "returns 0 for missing API key" do
      api_versions = %{10 => {0, 5}}
      preferred = [0, 1, 2]

      assert 0 = CodecBinary.pick_supported_version(api_versions, 99, preferred)
    end
  end

  describe "integration scenarios" do
    test "round-trip request/response for handshake" do
      corr = 12345
      mech = "SCRAM-SHA-256"

      # Build request
      request = CodecBinary.handshake_request(mech, corr, 1)
      assert is_binary(request)

      # Simulate broker response (v1 with mechanism list)
      mechanisms = ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"]
      mech_bytes = for m <- mechanisms, into: <<>>, do: <<byte_size(m)::16, m::binary>>
      response = <<corr::32, 0::16, length(mechanisms)::32, mech_bytes::binary>>

      # Parse response
      assert :ok = CodecBinary.parse_handshake_response(response, corr, mech, 1)
    end

    test "handles authentication flow with server challenges" do
      corr = 54321

      # First auth request
      client_first = "n,,n=user,r=rOprNGfwEbeRWgbNEkqO"
      request1 = CodecBinary.authenticate_request(client_first, corr, 0)
      assert is_binary(request1)

      # Server responds with challenge
      server_first = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF,s=W22ZaJ0SNY,i=4096"
      response1 = <<corr::32, 0::16, byte_size(server_first)::32, server_first::binary>>

      assert {:ok, ^server_first} = CodecBinary.parse_authenticate_response(response1, corr, 0)

      # Continue with final message...
      corr2 = corr + 1
      client_final = "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF,p=..."
      request2 = CodecBinary.authenticate_request(client_final, corr2, 0)
      assert is_binary(request2)
    end
  end

  describe "error handling edge cases" do
    test "handles malformed responses gracefully" do
      # Too short to parse
      assert {:error, :incomplete_response} = CodecBinary.parse_api_versions_response(<<1, 2>>, 1)

      # Short response even with matching correlation returns incomplete error
      assert {:error, :incomplete_response} =
               CodecBinary.parse_api_versions_response(<<123::32>>, 123)

      # Short response with wrong correlation - checks correlation first
      assert {:error, :correlation_mismatch} =
               CodecBinary.parse_api_versions_response(<<456::32>>, 123)
    end

    test "handles invalid mechanism list parsing" do
      corr = 999
      # Create a response with invalid data that will fail to parse properly
      # Using a huge count that doesn't match the actual data
      response = <<corr::32, 0::16, 999::32, "SHORT"::binary>>

      # The parser should return incomplete/empty mechanism list gracefully
      result = CodecBinary.parse_handshake_response(response, corr, "PLAIN", 1)
      assert {:error, {:unsupported_mechanism_on_broker, "PLAIN"}} = result
    end
  end

  describe "parse_authenticate_response/3 v2 (flexible)" do
    test "parses v2 success with compact bytes and session lifetime" do
      corr = 4242
      # build response:
      # corr::32
      # response-header tagged fields: <<0>>
      # error_code::16 = 0
      # error_message: compact_nullable_string(nil) -> <<0>>
      # sasl_auth_bytes: "ok" (len=2 -> <<3,"ok">>)
      # session_lifetime_ms::64
      # body tagged fields: <<0>>
      auth = "ok"
      session_ms = 1234

      # correlation id
      payload =
        <<
          corr::32,
          # resp hdr tagged fields (empty)
          0,
          # error_code = success
          0::16,
          # compact_nullable_string(nil)
          0,
          # compact_bytes("ok")
          3,
          auth::binary,
          session_ms::64,
          # body tagged fields (empty)
          0
        >>

      assert {:ok, ^auth} = CodecBinary.parse_authenticate_response(payload, corr, 2)
    end

    test "parses v2 success with empty auth bytes" do
      corr = 4243
      # auth bytes empty => compact_bytes(<<>>) == <<1>>
      payload =
        <<corr::32, 0, 0::16, 0, 1, 0::64, 0>>

      assert {:ok, <<>>} = CodecBinary.parse_authenticate_response(payload, corr, 2)
    end

    test "parses v2 auth failure with error message" do
      corr = 5001
      # error_code 58 = :sasl_authentication_failed
      # error_message "bad" -> len=3 => <<4, "bad">>
      err_msg = "bad"
      # corr
      payload =
        <<
          corr::32,
          # resp hdr tags
          0,
          # error_code
          58::16,
          # compact_nullable_string("bad")
          4,
          err_msg::binary,
          # compact_bytes(<<>>) to keep it simple (not used on error)
          1,
          # session lifetime ms
          0::64,
          # body tags
          0
        >>

      assert {:error, {:auth_failed, :sasl_authentication_failed, ^err_msg, 0}} =
               CodecBinary.parse_authenticate_response(payload, corr, 2)
    end

    test "returns correlation_mismatch for v2 when corr id differs" do
      corr = 9000
      other = 9001
      payload = <<other::32, 0, 0::16, 0, 1, 0::64, 0>>

      assert {:error, :correlation_mismatch} =
               CodecBinary.parse_authenticate_response(payload, corr, 2)
    end
  end
end
