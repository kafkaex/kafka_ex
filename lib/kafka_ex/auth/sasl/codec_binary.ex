defmodule KafkaEx.Auth.SASL.CodecBinary do
  @moduledoc false
  # Binary codec implementation for SASL protocol messages
  #
  # Handles the low-level wire protocol for SASL authentication:
  #
  # ## Request Building
  #   - API Versions (key=18): Query broker capabilities
  #   - SASL Handshake (key=17): Negotiate authentication mechanism
  #   - SASL Authenticate (key=36): Exchange authentication data
  #
  # ## Response Parsing
  #   - Validates correlation IDs to match requests with responses
  #   - Handles nullable fields and arrays per Kafka protocol
  #   - Maps error codes to descriptive atoms
  #
  # ## Version Selection
  #   - Picks highest mutually-supported version for each API
  #   - Falls back to v0 for older brokers
  #   - Special handling for v2+ flexible encoding (compact types)
  #
  # ## Protocol Details
  #
  # ### Fixed-length encoding (v0-v1):
  #   - Standard Kafka protocol: length-prefixed messages
  #   - Types: int16, int32, bytes, nullable_string, arrays
  #
  # ### Flexible encoding (v2+):
  #   - Compact types with varint encoding
  #   - Tagged fields for forward compatibility
  #   - Currently only SaslAuthenticate supports v2
  #
  # ## Error Handling
  #   - Returns {:error, :incomplete_response} for truncated data
  #   - Returns {:error, :correlation_mismatch} for wrong correlation ID
  #   - Maps Kafka error codes to atoms (e.g., 58 -> :sasl_authentication_failed)

  @behaviour KafkaEx.Auth.SASL.Codec

  import Bitwise

  # protocol mappings
  @api_versions_key 18
  @sasl_handshake_key 17
  @sasl_authenticate_key 36

  @handshake_versions [0, 1]
  @authenticate_versions [0, 1, 2]

  # ---------- Builders ----------

  @spec api_versions_request(non_neg_integer(), non_neg_integer(), binary()) :: binary()
  @impl true
  def api_versions_request(corr, ver, client_id \\ "kafka_ex") do
    <<@api_versions_key::16, ver::16, corr::32, byte_size(client_id)::16, client_id::binary>>
  end

  @spec handshake_request(binary(), non_neg_integer(), non_neg_integer(), binary()) :: binary()
  @impl true
  def handshake_request(mech, corr, ver, client_id \\ "kafka_ex") do
    <<@sasl_handshake_key::16, ver::16, corr::32, byte_size(client_id)::16, client_id::binary, byte_size(mech)::16,
      mech::binary>>
  end

  @spec authenticate_request(binary() | nil, non_neg_integer(), non_neg_integer(), binary()) :: binary()
  @impl true
  def authenticate_request(auth_bytes, corr, ver, client_id \\ "kafka_ex")

  def authenticate_request(nil, _corr, ver, _cid) when ver >= 2 do
    raise(ArgumentError, "SaslAuthenticate v#{ver} requires non-nil auth_bytes")
  end

  # v2+ (flexible): header v2 + compact body + tagged fields
  def authenticate_request(auth_bytes, corr, ver, client_id) when ver >= 2 do
    header =
      IO.iodata_to_binary([
        <<@sasl_authenticate_key::16, ver::16, corr::32>>,
        compact_string(client_id),
        write_tagged_fields_empty()
      ])

    body = IO.iodata_to_binary([compact_bytes(auth_bytes), write_tagged_fields_empty()])

    <<header::binary, body::binary>>
  end

  # v0/v1 - existing encoding stays as-is
  def authenticate_request(auth_bytes, corr, ver, client_id) do
    <<
      @sasl_authenticate_key::16,
      ver::16,
      corr::32,
      byte_size(client_id)::16,
      client_id::binary,
      byte_size(auth_bytes)::32,
      auth_bytes::binary
    >>
  end

  # ---------- Parsers  ----------

  @spec parse_api_versions_response(binary(), non_neg_integer()) :: map() | {:error, atom()}
  @impl true
  def parse_api_versions_response(data, _expected_corr) when byte_size(data) < 4 do
    # Too short to even have a correlation ID
    {:error, :incomplete_response}
  end

  def parse_api_versions_response(<<corr::32, rest::binary>>, expected_corr) do
    if corr != expected_corr do
      {:error, :correlation_mismatch}
    else
      parse_api_versions_body(rest)
    end
  end

  defp parse_api_versions_body(data) when byte_size(data) < 6 do
    # Need at least error_code (2) + array_count (4)
    {:error, :incomplete_response}
  end

  defp parse_api_versions_body(<<error_code::16, array_count::32, rest::binary>>) do
    if error_code != 0 do
      {:error, kafka_error_name(error_code)}
    else
      parse_api_array(array_count, rest, %{})
    end
  end

  defp parse_api_array(0, _rest, acc), do: acc
  defp parse_api_array(_n, data, acc) when byte_size(data) < 6, do: acc

  defp parse_api_array(n, <<key::16, min::16, max::16, rest::binary>>, acc) do
    parse_api_array(n - 1, rest, Map.put(acc, key, {min, max}))
  end

  @spec parse_handshake_response(binary(), non_neg_integer(), binary(), non_neg_integer()) :: :ok | {:error, term()}
  @impl true
  def parse_handshake_response(data, _expected_corr, _mech, _ver) when byte_size(data) < 4 do
    {:error, :incomplete_response}
  end

  def parse_handshake_response(<<corr::32, rest::binary>>, expected_corr, mech, ver) do
    if corr != expected_corr do
      {:error, :correlation_mismatch}
    else
      parse_handshake_body(rest, mech, ver)
    end
  end

  defp parse_handshake_body(data, _mech, _ver) when byte_size(data) < 2 do
    {:error, :incomplete_response}
  end

  defp parse_handshake_body(<<error_code::16, rest::binary>>, mech, ver) do
    cond do
      error_code != 0 -> {:error, {:handshake_failed, kafka_error_name(error_code)}}
      ver == 0 -> :ok
      ver >= 1 -> verify_mechanisms(rest, mech)
      true -> :ok
    end
  end

  @spec parse_authenticate_response(binary(), non_neg_integer(), non_neg_integer()) ::
          {:ok, binary() | nil} | {:error, term()}
  @impl true
  def parse_authenticate_response(data, _expected_corr, _ver) when byte_size(data) < 4,
    do: {:error, :incomplete_response}

  # v2+ (flexible response header: corr::int32, tagged_fields)
  def parse_authenticate_response(<<corr::32, rest::binary>>, expected_corr, ver) when ver >= 2 do
    if corr != expected_corr do
      {:error, :correlation_mismatch}
    else
      # skip response-header tagged fields
      body0 = read_tagged_fields(rest)

      # body: error_code::int16, error_message::compact_nullable_string,
      #       sasl_auth_bytes::compact_bytes, session_lifetime_ms::int64, tagged_fields
      <<error_code::16, body1::binary>> = body0
      {err_msg, body2} = read_compact_nullable_string(body1)
      {auth_bytes, body3} = read_compact_bytes(body2)
      <<session_ms::64, body4::binary>> = body3
      _tail = read_tagged_fields(body4)

      if error_code != 0 do
        {:error, {:auth_failed, kafka_error_name(error_code), err_msg, session_ms}}
      else
        {:ok, auth_bytes}
      end
    end
  end

  # v0/v1
  def parse_authenticate_response(<<corr::32, rest::binary>>, expected_corr, ver) do
    if corr != expected_corr, do: {:error, :correlation_mismatch}, else: parse_authenticate_body(rest, ver)
  end

  defp parse_authenticate_body(data, _ver) when byte_size(data) < 2 do
    {:error, :incomplete_response}
  end

  defp parse_authenticate_body(<<error_code::16, rest::binary>>, ver) do
    if error_code != 0 do
      {:error, {:auth_failed, kafka_error_name(error_code)}}
    else
      parse_authenticate_success(rest, ver)
    end
  end

  defp parse_authenticate_success(data, 0) do
    # v0: just auth_bytes
    parse_bytes(data)
  end

  defp parse_authenticate_success(data, ver) when ver >= 1 do
    # v1+: error_message then auth_bytes
    case parse_nullable_string(data) do
      {:error, _} -> {:error, :invalid_response}
      {_error_msg, rest} -> parse_bytes(rest)
    end
  end

  defp verify_mechanisms(data, _mech) when byte_size(data) < 4 do
    {:error, :incomplete_response}
  end

  defp verify_mechanisms(<<count::32, rest::binary>>, mech) do
    {mechs, _rest} = parse_string_array(count, rest, [])

    if mech in mechs do
      :ok
    else
      {:error, {:unsupported_mechanism_on_broker, mech}}
    end
  end

  defp parse_string_array(0, rest, acc), do: {Enum.reverse(acc), rest}

  defp parse_string_array(_n, data, acc) when byte_size(data) < 2 do
    # Incomplete array - return what we have so far
    {Enum.reverse(acc), data}
  end

  defp parse_string_array(n, <<len::16, rest::binary>>, acc) do
    if byte_size(rest) < len do
      # Not enough data for this string
      {Enum.reverse(acc), <<len::16, rest::binary>>}
    else
      <<str::binary-size(len), rest2::binary>> = rest
      parse_string_array(n - 1, rest2, [str | acc])
    end
  end

  defp parse_nullable_string(data) when byte_size(data) < 2 do
    {:error, :incomplete}
  end

  defp parse_nullable_string(<<-1::16-signed, rest::binary>>), do: {nil, rest}

  defp parse_nullable_string(<<len::16, rest::binary>>) do
    if byte_size(rest) < len do
      {:error, :incomplete}
    else
      <<str::binary-size(len), rest::binary>> = rest
      {str, rest}
    end
  end

  defp parse_bytes(data) when byte_size(data) < 4 do
    {:error, :incomplete}
  end

  defp parse_bytes(<<-1::32-signed, _rest::binary>>), do: {:ok, nil}

  defp parse_bytes(<<len::32, rest::binary>>) do
    if byte_size(rest) < len do
      {:error, :incomplete}
    else
      <<bytes::binary-size(len), _rest::binary>> = rest
      {:ok, bytes}
    end
  end

  @spec kafka_error_name(non_neg_integer()) :: atom() | {:error_code, non_neg_integer()}
  def kafka_error_name(33), do: :unsupported_sasl_mechanism
  def kafka_error_name(34), do: :illegal_sasl_state
  def kafka_error_name(35), do: :unsupported_version
  def kafka_error_name(58), do: :sasl_authentication_failed
  def kafka_error_name(code), do: {:error_code, code}

  # ---------- Version picking ----------

  @doc """
  Pick the highest preferred version within the broker's {min,max} for SASL_HANDSHAKE.
  """
  @spec pick_handshake_version(map() | {:error, term()}) :: non_neg_integer()
  @impl true
  def pick_handshake_version({:error, _}), do: 0

  def pick_handshake_version(api_versions) do
    pick_supported_version(api_versions, @sasl_handshake_key, @handshake_versions)
  end

  @doc """
  Pick the highest preferred version within the broker's {min,max} for SASL_AUTHENTICATE.
  """
  @spec pick_authenticate_version(map() | {:error, term()}) :: non_neg_integer()
  @impl true
  def pick_authenticate_version({:error, _}), do: 0

  def pick_authenticate_version(api_versions) do
    pick_supported_version(api_versions, @sasl_authenticate_key, @authenticate_versions)
  end

  @doc """
  Generic version picker used by the specific helpers above.
  """
  @spec pick_supported_version(map(), non_neg_integer(), [non_neg_integer()]) :: non_neg_integer()
  def pick_supported_version(api_versions, api_key, preferred_versions) do
    case Map.get(api_versions, api_key) do
      nil -> 0
      {min_v, max_v} -> preferred_versions |> Enum.filter(&(&1 >= min_v and &1 <= max_v)) |> Enum.max(fn -> 0 end)
    end
  end

  # ------- Flexible (v2) helpers -------

  # UnsignedVarInt (7-bit groups, MSB=continuation)
  defp uvarint_encode(n) when n >= 0, do: do_uvi(n, [])
  defp do_uvi(n, acc) when n < 0x80, do: IO.iodata_to_binary(Enum.reverse([<<n>> | acc]))
  defp do_uvi(n, acc), do: do_uvi(n >>> 7, [<<(n &&& 0x7F) ||| 0x80>> | acc])

  defp uvarint_decode(<<byte, rest::binary>>, shift \\ 0, acc \\ 0) do
    v = acc ||| (byte &&& 0x7F) <<< shift
    if (byte &&& 0x80) == 0, do: {v, rest}, else: uvarint_decode(rest, shift + 7, v)
  end

  defp compact_bytes(nil), do: <<0>>
  defp compact_bytes(<<>>), do: <<1>>

  defp compact_bytes(bin) when is_binary(bin) do
    IO.iodata_to_binary([uvarint_encode(byte_size(bin) + 1), bin])
  end

  defp compact_string(nil) do
    raise(ArgumentError, "Field is non-nullable (COMPACT_STRING)")
  end

  defp compact_string(str) when is_binary(str) do
    bin = :unicode.characters_to_binary(str)
    IO.iodata_to_binary([uvarint_encode(byte_size(bin) + 1), bin])
  end

  defp read_compact_nullable_string(<<0, rest::binary>>), do: {nil, rest}
  defp read_compact_nullable_string(<<1, rest::binary>>), do: {"", rest}

  defp read_compact_nullable_string(bin) do
    {len_plus_1, after_len} = uvarint_decode(bin)
    len = len_plus_1 - 1
    <<s::binary-size(len), rest::binary>> = after_len
    {s, rest}
  end

  defp read_compact_bytes(<<0, rest::binary>>), do: {nil, rest}
  # Empty bytes
  defp read_compact_bytes(<<1, rest::binary>>), do: {<<>>, rest}

  defp read_compact_bytes(bin) do
    {len_plus_1, after_len} = uvarint_decode(bin)
    len = len_plus_1 - 1
    <<payload::binary-size(len), rest::binary>> = after_len
    {payload, rest}
  end

  # Tagged fields: for now we just write/read an empty set (0)
  defp write_tagged_fields_empty, do: <<0>>

  defp read_tagged_fields(bin) do
    {n, rest} = uvarint_decode(bin)
    # skip n tags (none expected for SaslAuthenticate v2 currently)
    skip_tags(n, rest)
  end

  defp skip_tags(0, rest), do: rest

  defp skip_tags(n, bin) do
    # tag_id
    {_, r1} = uvarint_decode(bin)
    # size
    {sz, r2} = uvarint_decode(r1)
    <<_val::binary-size(sz), r3::binary>> = r2
    skip_tags(n - 1, r3)
  end
end
