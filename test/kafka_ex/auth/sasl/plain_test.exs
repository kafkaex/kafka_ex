defmodule KafkaEx.Auth.SASL.PlainTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Auth.SASL.Plain
  alias KafkaEx.Auth.Config

  describe "mechanism_name/1" do
    test "returns PLAIN" do
      config = %Config{mechanism: :plain, username: "user", password: "pass"}
      assert Plain.mechanism_name(config) == "PLAIN"
    end
  end

  describe "authenticate/2" do
    test "sends correct PLAIN message format" do
      config = %Config{mechanism: :plain, username: "testuser", password: "testpass"}

      send_fun = fn msg ->
        # PLAIN format: <<0, username, 0, password>>
        assert msg == <<0, "testuser", 0, "testpass">>
        {:ok, nil}
      end

      assert :ok = Plain.authenticate(config, send_fun)
    end

    test "handles empty username and password" do
      config = %Config{mechanism: :plain, username: "", password: ""}

      send_fun = fn msg ->
        assert msg == <<0, 0>>
        {:ok, nil}
      end

      assert :ok = Plain.authenticate(config, send_fun)
    end

    test "handles special characters in credentials" do
      config = %Config{
        mechanism: :plain,
        username: "user@domain.com",
        password: "p@ss!w0rd#$%"
      }

      send_fun = fn msg ->
        assert msg == <<0, "user@domain.com", 0, "p@ss!w0rd#$%">>
        {:ok, nil}
      end

      assert :ok = Plain.authenticate(config, send_fun)
    end

    test "handles unicode in credentials" do
      config = %Config{
        mechanism: :plain,
        username: "用户",
        password: "密码"
      }

      send_fun = fn msg ->
        assert msg == <<0, "用户"::utf8, 0, "密码"::utf8>>
        {:ok, nil}
      end

      assert :ok = Plain.authenticate(config, send_fun)
    end

    test "returns error when send_fun fails" do
      config = %Config{mechanism: :plain, username: "user", password: "pass"}

      send_fun = fn _msg ->
        {:error, :connection_closed}
      end

      assert {:error, :connection_closed} = Plain.authenticate(config, send_fun)
    end

    test "returns error tuple from send_fun" do
      config = %Config{mechanism: :plain, username: "user", password: "pass"}

      send_fun = fn _msg ->
        {:error, {:auth_failed, "bad credentials"}}
      end

      assert {:error, {:auth_failed, "bad credentials"}} = Plain.authenticate(config, send_fun)
    end
  end
end
