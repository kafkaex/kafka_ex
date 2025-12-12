defmodule KafkaEx.Integration.OAuthBearerAuthenticationTest do
  use ExUnit.Case

  @moduletag :integration
  @moduletag :oauthbearer

  describe "SASL/OAUTHBEARER authentication" do
    @tag sasl: :oauthbearer
    test "connects and produces with unsecured JWT" do
      token = build_unsecured_jwt("test-user")

      opts = [
        uris: [{"localhost", 9394}],
        use_ssl: true,
        ssl_options: [verify: :verify_none],
        auth:
          KafkaEx.Auth.Config.new(%{
            mechanism: :oauthbearer,
            mechanism_opts: %{token_provider: fn -> {:ok, token} end}
          })
      ]

      {:ok, _pid} = KafkaEx.create_worker(:oauth_worker, opts)

      assert KafkaEx.produce("test_topic", 0, "oauth_msg", worker_name: :oauth_worker) == :ok
      # probably redundant, but whatever
      assert %KafkaEx.Protocol.Metadata.Response{} = KafkaEx.metadata(worker_name: :oauth_worker)
    end

    @tag sasl: :oauthbearer
    test "connects with extensions" do
      token = build_unsecured_jwt("test-user")

      opts = [
        uris: [{"localhost", 9394}],
        use_ssl: true,
        ssl_options: [verify: :verify_none],
        auth:
          KafkaEx.Auth.Config.new(%{
            mechanism: :oauthbearer,
            mechanism_opts: %{
              token_provider: fn -> {:ok, token} end,
              extensions: %{"traceId" => "test-123"}
            }
          })
      ]

      {:ok, _pid} = KafkaEx.create_worker(:oauth_ext_worker, opts)
      # probably redundant, but whatever
      assert %KafkaEx.Protocol.Metadata.Response{} = KafkaEx.metadata(worker_name: :oauth_ext_worker)
    end

    @tag sasl: :oauthbearer
    test "fails with invalid token" do
      opts = [
        uris: [{"localhost", 9394}],
        use_ssl: true,
        ssl_options: [verify: :verify_none],
        auth:
          KafkaEx.Auth.Config.new(%{
            mechanism: :oauthbearer,
            mechanism_opts: %{token_provider: fn -> {:ok, "invalid"} end}
          })
      ]

      assert {:error, _} = KafkaEx.create_worker(:oauth_bad_worker, opts)
    end
  end

  defp build_unsecured_jwt(subject) do
    # "alg":"none" as we have no jwks in tests
    header = Base.url_encode64(~s({"alg":"none","typ":"JWT"}), padding: false)
    now = System.system_time(:second)
    payload = Base.url_encode64(~s({"sub":"#{subject}","iat":#{now},"exp":#{now + 3600}}), padding: false)
    "#{header}.#{payload}."
  end
end
