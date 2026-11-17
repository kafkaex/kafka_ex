defmodule KafkaEx.TestSupport.FakeTransport.Wire do
  @moduledoc false

  alias Kayrock.KafkaSchemaMetadata
  alias Kayrock.Serialize

  @api_versions 18

  def api_key(api) when is_integer(api), do: api
  def api_key(api) when is_atom(api), do: KafkaSchemaMetadata.api_key(api)

  def api_name(api_key) do
    KafkaSchemaMetadata.api_key(api_key)
  rescue
    FunctionClauseError -> api_key
  end

  def parse_header(<<api_key::16-signed, api_version::16-signed, correlation_id::32-signed, rest::binary>>) do
    %{
      api_key: api_key,
      api: api_name(api_key),
      api_version: api_version,
      correlation_id: correlation_id,
      client_id: client_id(rest)
    }
  end

  defp client_id(<<size::16-signed, id::binary-size(size), _::binary>>) when size >= 0, do: id
  defp client_id(_), do: nil

  def response(api_key, api_version, correlation_id, fields) do
    schema = response_module(api_key, api_version).schema()
    [header(api_key, schema, correlation_id), encode_struct(schema, fields)]
  end

  def api_versions_fields(versions) do
    api_keys =
      versions
      |> Enum.sort()
      |> Enum.map(fn {key, first..last//_} -> %{api_key: key, min_version: first, max_version: last} end)

    %{error_code: 0, api_keys: api_keys}
  end

  def metadata_fields(brokers, topics) do
    %{
      brokers: Enum.map(brokers, fn {node_id, host, port} -> %{node_id: node_id, host: host, port: port} end),
      topics: Enum.map(topics, fn {name, leaders} -> %{error_code: 0, name: name, partitions: partitions(leaders)} end)
    }
  end

  defp partitions(leaders) do
    leaders
    |> Enum.with_index()
    |> Enum.map(fn {leader, index} ->
      %{error_code: 0, partition_index: index, leader_id: leader, replica_nodes: [leader], isr_nodes: [leader]}
    end)
  end

  defp response_module(api_key, version) do
    name = api_key |> api_name() |> Atom.to_string() |> Macro.camelize()
    Module.concat([Kayrock, name, :"V#{version}", Response])
  end

  # ApiVersions keeps the v0 response header even in its flexible versions.
  defp header(api_key, schema, correlation_id) do
    if Keyword.has_key?(schema, :tagged_fields) and api_key != @api_versions,
      do: [<<correlation_id::32-signed>>, 0],
      else: <<correlation_id::32-signed>>
  end

  defp encode_struct(schema, fields), do: Enum.map(schema, fn {name, type} -> encode(type, Map.get(fields, name)) end)

  defp encode(:tagged_fields, _value), do: <<0>>
  defp encode({array, type}, nil) when array in [:array, :compact_array], do: encode({array, type}, [])
  defp encode({:array, type}, values), do: [<<length(values)::32-signed>>, encode_items(type, values)]

  defp encode({:compact_array, type}, values),
    do: [Serialize.serialize(:unsigned_varint, length(values) + 1), encode_items(type, values)]

  defp encode(type, nil) when type in [:int8, :int16, :int32, :int64], do: Serialize.serialize(type, 0)
  defp encode(:boolean, nil), do: Serialize.serialize(:boolean, false)
  defp encode(type, value), do: Serialize.serialize(type, value)

  defp encode_items(type, values) when is_atom(type), do: Enum.map(values, &encode(type, &1))
  defp encode_items(schema, values), do: Enum.map(values, &encode_struct(schema, &1))
end
