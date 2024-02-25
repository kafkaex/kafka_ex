defmodule KafkaEx.Utils.Murmur do
  @moduledoc """
  Utility module that provides Murmur hashing algorithm.
  """

  import Bitwise

  # Arbitrary constant for murmur2 hashing
  # https://github.com/aappleby/smhasher/blob/master/src/MurmurHash2.cpp#L39-L43
  @m 0x5BD1E995
  @r 24

  # Default seed to hashing, copied form Java implementation
  # https://github.com/apache/kafka/blob/809be928f1ae004e11d65c307ea322bef126c834/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L382
  @seed 0x9747B28C

  @doc """
  Calculates murmur2 hash for given binary
  """
  @spec murmur2(key :: binary) :: integer
  def murmur2(key) do
    <<seed::signed-size(32)>> = <<@seed::size(32)>>
    len = byte_size(key)
    _murmur2(key, bxor(seed, len))
  end

  @doc """
  Calculates murmur2 hash for given binary as unsigned 32-bit integer
  """
  @spec umurmur2(key :: binary) :: integer
  def umurmur2(key) do
    key |> murmur2() |> band(0x7FFFFFFF)
  end

  @doc """
  Calculates murmur2 hash for given binary as unsigned 32-bit integer

  This is to support the legacy default partitioner implementation.
  """
  @spec umurmur2_legacy(key :: binary) :: integer
  def umurmur2_legacy(key) do
    key |> murmur2() |> band(0xFFFFFFFF)
  end

  defp mask32(num) do
    <<signed::signed-size(32)>> = <<num &&& 0xFFFFFFFF::size(32)>>
    signed
  end

  # Unsigned Bitwise right shift on 32 bits
  defp ubsr32(num, shift) do
    (num &&& 0xFFFFFFFF) >>> shift
  end

  defp _murmur2(<<a::little-size(32), rest::binary>>, h) do
    k = mask32(a * @m)
    k = bxor(k, ubsr32(k, @r))
    k = mask32(k * @m)
    h = mask32(h * @m)
    _murmur2(rest, bxor(h, k))
  end

  defp _murmur2(<<a1::size(8), a2::size(8), a3::size(8)>>, h) do
    _murmur2(<<a1, a2>>, bxor(h, mask32(a3 <<< 16)))
  end

  defp _murmur2(<<a1::size(8), a2::size(8)>>, h) do
    _murmur2(<<a1>>, bxor(h, mask32(a2 <<< 8)))
  end

  defp _murmur2(<<a1::size(8)>>, h) do
    _murmur2("", mask32(bxor(h, a1) * @m))
  end

  defp _murmur2("", h) do
    h = bxor(h, ubsr32(h, 13))
    h = mask32(h * @m)
    bxor(h, ubsr32(h, 15))
  end
end
