defmodule KafkaEx.Utils.MurmurTest do
  use ExUnit.Case

  alias KafkaEx.Utils.Murmur

  test "correctly encodes strings" do
    assert Murmur.murmur2("rule") == -1_673_595_344
    assert Murmur.umurmur2("rule") == 2_621_371_952

    assert Murmur.murmur2("monkey") == 385_264_353
    assert Murmur.umurmur2("monkey") == 385_264_353

    assert Murmur.murmur2("hover") == -1_982_829_826
    assert Murmur.umurmur2("hover") == 2_312_137_470

    assert Murmur.murmur2("guest") == 1_235_690_374
    assert Murmur.umurmur2("guest") == 1_235_690_374

    assert Murmur.murmur2("necklace") == -515_547_202
    assert Murmur.umurmur2("necklace") == 3_779_420_094

    assert Murmur.murmur2("storm") == 393_248_174
    assert Murmur.umurmur2("storm") == 393_248_174

    assert Murmur.murmur2("paint") == -1_653_751_818
    assert Murmur.umurmur2("paint") == 2_641_215_478

    assert Murmur.murmur2("agony") == -1_158_062_389
    assert Murmur.umurmur2("agony") == 3_136_904_907

    assert Murmur.murmur2("strategic") == -758_786_001
    assert Murmur.umurmur2("strategic") == 3_536_181_295

    assert Murmur.murmur2("redundancy") == 451_414_978
    assert Murmur.umurmur2("redundancy") == 451_414_978
  end
end
