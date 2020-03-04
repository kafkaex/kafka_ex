defmodule KafkaEx.Utils.MurmurTest do
  use ExUnit.Case

  alias KafkaEx.Utils.Murmur

  test "correctly encodes strings" do
    assert Murmur.murmur2("rule") == -1_673_595_344
    assert Murmur.umurmur2("rule") == 473_888_304

    assert Murmur.murmur2("monkey") == 385_264_353
    assert Murmur.umurmur2("monkey") == 385_264_353

    assert Murmur.murmur2("hover") == -1_982_829_826
    assert Murmur.umurmur2("hover") == 164_653_822

    assert Murmur.murmur2("guest") == 1_235_690_374
    assert Murmur.umurmur2("guest") == 1_235_690_374

    assert Murmur.murmur2("necklace") == -515_547_202
    assert Murmur.umurmur2("necklace") == 1_631_936_446

    assert Murmur.murmur2("storm") == 393_248_174
    assert Murmur.umurmur2("storm") == 393_248_174

    assert Murmur.murmur2("paint") == -1_653_751_818
    assert Murmur.umurmur2("paint") == 493_731_830

    assert Murmur.murmur2("agony") == -1_158_062_389
    assert Murmur.umurmur2("agony") == 989_421_259

    assert Murmur.murmur2("strategic") == -758_786_001
    assert Murmur.umurmur2("strategic") == 1_388_697_647

    assert Murmur.murmur2("redundancy") == 451_414_978
    assert Murmur.umurmur2("redundancy") == 451_414_978
  end
end
