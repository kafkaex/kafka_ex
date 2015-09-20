defmodule KafkaEx.Compression do
  @moduledoc """
  Handles compression/decompression of messages.

  See https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Compression

  To add new compression types:

  1. Add the appropriate dependency to mix.exs (don't forget to add it
  to the application list). 
  2. Add the appropriate attribute value and compression_type atom.
  3. Add a decompress function clause.
  4. Add a compress function clause.

  """

  @snappy_attribute 2

  @type attribute_t :: integer
  @type compression_type_t :: :snappy

  @doc """
  This function should pattern match on the attribute value and return
  the decompressed data.
  """
  @spec decompress(attribute_t, binary) :: binary
  def decompress(@snappy_attribute, data) do
    << _snappy_header :: 64, _snappy_version_info :: 64,
    valsize :: 32, value :: size(valsize)-binary >> = data
    {:ok, decompressed} = :snappy.decompress(value)
    decompressed
  end

  @doc """
  This function should pattern match on the compression_type atom and
  return the compressed data as well as the corresponding attribute
  value.
  """
  @spec compress(compression_type_t, binary) :: {binary, attribute_t}
  def compress(:snappy, data) do
    {:ok, compressed_data} = :snappy.compress(data)
    {compressed_data, @snappy_attribute}
  end
end
