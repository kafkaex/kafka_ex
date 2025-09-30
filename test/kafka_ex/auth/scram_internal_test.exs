defmodule KafkaEx.Auth.ScramInternalTest do
  use ExUnit.Case, async: true

  @moduletag :sasl
  
  alias KafkaEx.Auth.ScramFlow.Internal
  
  describe "client_first_message/1" do
    test "generates correct format" do
      scram = %Internal{
        username: "alice",
        client_nonce: "rOprNGfwEbeRWgbNEkqO"
      }
      
      {message, updated} = Internal.client_first(scram)
      
      assert message == "n,,n=alice,r=rOprNGfwEbeRWgbNEkqO"
      assert updated.client_first_bare == "n=alice,r=rOprNGfwEbeRWgbNEkqO"
    end
    
    test "escapes special characters" do
      scram = %Internal{
        username: "user=name,test",
        client_nonce: "nonce"
      }
      
      {message, _} = Internal.client_first(scram)
      
      assert message == "n,,n=user=3Dname=2Ctest,r=nonce"
    end
  end
  
  describe "handle_server_first/2" do
    test "stores exact server message" do
      scram = %Internal{
        client_nonce: "clientnonce"
      }
      
      # Different attribute order to test exact storage
      server_first = "i=4096,s=QSXCR+Q6sek8bf92,r=clientnonceservernonce"
      
      {:ok, updated} = Internal.handle_server_first(scram, server_first)
      
      assert updated.server_first_raw == server_first
      assert updated.iterations == 4096
      assert updated.server_nonce == "clientnonceservernonce"
    end
    
    test "validates server nonce" do
      scram = %Internal{client_nonce: "clientnonce"}
      server_first = "r=wrongnonce,s=salt,i=4096"
      
      assert {:error, :invalid_server_nonce} = 
        Internal.handle_server_first(scram, server_first)
    end
  end
  
  describe "RFC 7677 test vectors" do
    test "SCRAM-SHA-256 exact proof" do
      scram = %Internal{
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
      
      {client_final, updated} = Internal.client_final(scram)
      
      # RFC expected values
      expected_proof = "dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
      expected_sig = "6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="
      
      assert client_final =~ "p=#{expected_proof}"
      assert Base.encode64(updated.server_signature) == expected_sig
    end
  end
  
  describe "verify_server_final/2" do
    test "accepts valid signature" do
      server_sig = "validSignature123"
      scram = %Internal{
        server_signature: server_sig
      }
      
      server_final = "v=" <> Base.encode64(server_sig)
      
      assert :ok = Internal.verify_server_final(scram, server_final)
    end
        
    test "rejects invalid signature" do
      scram = %Internal{
        server_signature: "expected_signature"  # Just use plain string
      }
      
      server_final = "v=" <> Base.encode64("wrong_signature")
      
      assert {:error, :invalid_server_signature} = 
        Internal.verify_server_final(scram, server_final)
    end

    test "handles server error" do
      scram = %Internal{}
      server_final = "e=invalid-auth"
      
      assert {:error, {:server_error, "invalid-auth"}} = 
        Internal.verify_server_final(scram, server_final)
    end
  end
end
