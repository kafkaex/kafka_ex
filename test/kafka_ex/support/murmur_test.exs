defmodule KafkaEx.Support.MurmurTest do
  use ExUnit.Case

  alias KafkaEx.Support.Murmur

  test "murmur2 correctly encodes strings" do
    # Taken from https://github.com/apache/kafka/blob/8ab0994919752cd4870e771221ba934a6a539a67/clients/src/test/java/org/apache/kafka/common/utils/UtilsTest.java#L66-L78
    assert Murmur.murmur2("21") == -973_932_308
    assert Murmur.murmur2("foobar") == -790_332_482
    assert Murmur.murmur2("a-little-bit-long-string") == -985_981_536
    assert Murmur.murmur2("a-little-bit-longer-string") == -1_486_304_829

    assert Murmur.murmur2("lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8") ==
             -58_897_971

    assert Murmur.murmur2("abc") == 479_470_107
  end

  test "umurmur2 correctly encodes strings" do
    assert Murmur.umurmur2("rule") == 473_888_304
    assert Murmur.umurmur2("monkey") == 385_264_353
    assert Murmur.umurmur2("hover") == 164_653_822
    assert Murmur.umurmur2("guest") == 1_235_690_374
    assert Murmur.umurmur2("necklace") == 1_631_936_446
    assert Murmur.umurmur2("storm") == 393_248_174
    assert Murmur.umurmur2("paint") == 493_731_830
    assert Murmur.umurmur2("agony") == 989_421_259
    assert Murmur.umurmur2("strategic") == 1_388_697_647
    assert Murmur.umurmur2("redundancy") == 451_414_978
  end

  test "legacy umurmur2 correctly encodes strings" do
    assert Murmur.umurmur2_legacy("rule") == 2_621_371_952
    assert Murmur.umurmur2_legacy("monkey") == 385_264_353
    assert Murmur.umurmur2_legacy("hover") == 2_312_137_470
    assert Murmur.umurmur2_legacy("guest") == 1_235_690_374
    assert Murmur.umurmur2_legacy("necklace") == 3_779_420_094
    assert Murmur.umurmur2_legacy("storm") == 393_248_174
    assert Murmur.umurmur2_legacy("paint") == 2_641_215_478
    assert Murmur.umurmur2_legacy("agony") == 3_136_904_907
    assert Murmur.umurmur2_legacy("strategic") == 3_536_181_295
    assert Murmur.umurmur2_legacy("redundancy") == 451_414_978
  end
end
