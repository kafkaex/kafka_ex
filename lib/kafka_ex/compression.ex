defmodule KafkaEx.Compression do
  @snappy_attribute 2

  @type attribute_t :: integer

  @spec decompress(attribute_t, binary) :: binary
  def decompress(@snappy_attribute, data) do
    << _snappy_header :: 64, _snappy_version_info :: 64,
    valsize :: 32, value :: size(valsize)-binary >> = data
    {:ok, decompressed} = :snappy.decompress(value)
    decompressed
  end
end
