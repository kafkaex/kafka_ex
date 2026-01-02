defmodule KafkaEx.Auth.MechanismTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Auth.Mechanism

  describe "behaviour" do
    test "defines mechanism_name callback" do
      callbacks = Mechanism.behaviour_info(:callbacks)
      assert {:mechanism_name, 1} in callbacks
    end

    test "defines authenticate callback" do
      callbacks = Mechanism.behaviour_info(:callbacks)
      assert {:authenticate, 2} in callbacks
    end
  end

  describe "built-in implementations" do
    test "Plain implements Mechanism behaviour" do
      assert KafkaEx.Auth.SASL.Plain.module_info(:attributes)[:behaviour] |> List.flatten() |> Enum.member?(Mechanism)
    end

    test "Scram implements Mechanism behaviour" do
      assert KafkaEx.Auth.SASL.Scram.module_info(:attributes)[:behaviour] |> List.flatten() |> Enum.member?(Mechanism)
    end
  end
end
