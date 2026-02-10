defmodule KafkaEx.Auth.ScramFlow.InternalTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Auth.ScramFlow.Internal

  describe "client_first/1" do
    test "generates client-first message" do
      state = %Internal{
        algorithm: :sha256,
        username: "testuser",
        password: "password",
        client_nonce: "rOprNGfwEbeRWgbNEkqO"
      }

      {message, new_state} = Internal.client_first(state)

      assert String.starts_with?(message, "n,,n=testuser,r=")
      assert String.contains?(message, "rOprNGfwEbeRWgbNEkqO")
      assert new_state.client_first_bare == "n=testuser,r=rOprNGfwEbeRWgbNEkqO"
    end

    test "escapes special characters in username" do
      state = %Internal{
        algorithm: :sha256,
        username: "user=name,test",
        password: "password",
        client_nonce: "abc123"
      }

      {message, _new_state} = Internal.client_first(state)

      # "=" becomes "=3D", "," becomes "=2C"
      assert String.contains?(message, "n=user=3Dname=2Ctest")
    end
  end

  describe "handle_server_first/2" do
    test "parses valid server-first message" do
      state = %Internal{
        algorithm: :sha256,
        username: "testuser",
        password: "password",
        client_nonce: "clientnonce"
      }

      server_first = "r=clientnonceserverpart,s=c2FsdA==,i=4096"

      assert {:ok, new_state} = Internal.handle_server_first(state, server_first)
      assert new_state.server_first_raw == server_first
      assert new_state.server_nonce == "clientnonceserverpart"
      assert new_state.salt == "salt"
      assert new_state.iterations == 4096
    end

    test "returns error when server nonce doesn't start with client nonce" do
      state = %Internal{
        algorithm: :sha256,
        username: "testuser",
        password: "password",
        client_nonce: "clientnonce"
      }

      server_first = "r=differentnonce,s=c2FsdA==,i=4096"

      assert {:error, :invalid_server_nonce} = Internal.handle_server_first(state, server_first)
    end

    test "returns error for malformed server-first message" do
      state = %Internal{
        algorithm: :sha256,
        username: "testuser",
        password: "password",
        client_nonce: "clientnonce"
      }

      # Missing required fields
      server_first = "r=clientnonce,invalid=data"

      assert {:error, :invalid_server_first_message} = Internal.handle_server_first(state, server_first)
    end
  end

  describe "client_final/1" do
    test "generates client-final message with proof" do
      state = %Internal{
        algorithm: :sha256,
        username: "user",
        password: "pencil",
        client_nonce: "rOprNGfwEbeRWgbNEkqO",
        client_first_bare: "n=user,r=rOprNGfwEbeRWgbNEkqO",
        server_first_raw: "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096",
        server_nonce: "rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0",
        salt: Base.decode64!("W22ZaJ0SNY7soEsUEjb6gQ=="),
        iterations: 4096
      }

      {message, new_state} = Internal.client_final(state)

      # Message should contain channel binding, nonce, and proof
      assert String.contains?(message, "c=biws")
      assert String.contains?(message, "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0")
      assert String.contains?(message, "p=")
      assert new_state.auth_message != nil
      assert new_state.server_signature != nil
    end
  end

  describe "verify_server_final/2" do
    test "returns error for invalid signature" do
      state = %Internal{
        algorithm: :sha256,
        server_signature: "some_signature"
      }

      server_final = "v=#{Base.encode64("wrong_signature")}"

      assert {:error, :invalid_server_signature} = Internal.verify_server_final(state, server_final)
    end

    test "verifies correct signature" do
      signature = :crypto.strong_rand_bytes(32)

      state = %Internal{
        algorithm: :sha256,
        server_signature: signature
      }

      server_final = "v=#{Base.encode64(signature)}"

      assert :ok = Internal.verify_server_final(state, server_final)
    end

    test "returns server error when present" do
      state = %Internal{algorithm: :sha256, server_signature: "sig"}

      server_final = "e=invalid-encoding"

      assert {:error, {:server_error, "invalid-encoding"}} = Internal.verify_server_final(state, server_final)
    end

    test "returns error for malformed server-final message" do
      state = %Internal{algorithm: :sha256, server_signature: "sig"}

      server_final = "invalid"

      assert {:error, :invalid_server_final_message} = Internal.verify_server_final(state, server_final)
    end
  end

  describe "SCRAM with SHA-512" do
    test "works with sha512 algorithm" do
      state = %Internal{
        algorithm: :sha512,
        username: "testuser",
        password: "password",
        client_nonce: "testnonce"
      }

      {message, new_state} = Internal.client_first(state)

      assert String.starts_with?(message, "n,,")
      assert new_state.client_first_bare != nil
    end
  end
end
