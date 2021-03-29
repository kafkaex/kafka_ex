defmodule CompressionTest do
  use ExUnit.Case

  test "snappy decompression works with chunked messages" do
    data =
      <<130, 83, 78, 65, 80, 80, 89, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 14, 12,
        44, 0, 0, 0, 0, 0, 0, 0, 37, 0, 0, 3, 246, 0, 0, 0, 75, 246, 7, 92, 10,
        44, 16, 236, 0, 0, 255, 255, 255, 255, 0, 0, 3, 232, 65, 66, 67, 68, 69,
        70, 71, 72, 73, 74, 254, 10, 0, 254, 10, 0, 254, 10, 0, 254, 10, 0, 254,
        10, 0, 254, 10, 0, 254, 10, 0, 254, 10, 0, 254, 10, 0, 254, 10, 0, 254,
        10, 0, 254, 10, 0, 254, 10, 0, 254, 10, 0, 254, 10, 0, 118, 10, 0>>

    expected =
      <<0, 0, 0, 0, 0, 0, 0, 37, 0, 0, 3, 246, 10, 44, 16, 236, 0, 0, 255, 255,
        255, 255, 0, 0, 3, 232>> <> String.duplicate("ABCDEFGHIJ", 100)

    ## enable :snappy module, and test it
    Application.put_env(:kafka_ex, :snappy_module, :snappy)
    assert expected == KafkaEx.Compression.decompress(2, data)

    ## enable :snappyer module, and test it
    Application.put_env(:kafka_ex, :snappy_module, :snappyer)
    assert expected == KafkaEx.Compression.decompress(2, data)
  end
end
