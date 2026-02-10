defmodule KafkaEx.Integration.Auth.OAuthBearerTest do
  @moduledoc """
  Integration tests for SASL/OAUTHBEARER authentication (KIP-255, KIP-342).
  Tests JWT-based authentication with token providers.
  """
  use ExUnit.Case
  @moduletag :auth
  @moduletag :oauthbearer

  import KafkaEx.TestHelpers

  alias KafkaEx.API
  alias KafkaEx.Client

  @oauth_port 9394

  describe "OAUTHBEARER connection" do
    test "connects successfully with valid JWT" do
      {:ok, opts} = oauth_opts()
      {:ok, pid} = Client.start_link(opts, :no_name)

      assert Process.alive?(pid)

      {:ok, metadata} = API.metadata(pid)
      assert %KafkaEx.Cluster.ClusterMetadata{} = metadata
    end

    test "fails with invalid token" do
      {:ok, opts} = oauth_opts(token: "invalid-not-a-jwt")

      assert_auth_failure(fn -> Client.start_link(opts, :no_name) end)
    end

    test "fails with expired token" do
      expired_token = build_jwt("test-user", exp_offset: -3600)
      {:ok, opts} = oauth_opts(token: expired_token)

      assert_auth_failure(fn -> Client.start_link(opts, :no_name) end)
    end

    test "fails when token provider returns error" do
      {:ok, opts} = oauth_opts(token_provider: fn -> {:error, :token_fetch_failed} end)

      assert_auth_failure(fn -> Client.start_link(opts, :no_name) end)
    end
  end

  describe "OAUTHBEARER with extensions" do
    test "connects with KIP-342 extensions" do
      {:ok, opts} = oauth_opts(extensions: %{"traceId" => "test-trace-123", "env" => "test"})
      {:ok, pid} = Client.start_link(opts, :no_name)

      {:ok, metadata} = API.metadata(pid)
      assert %KafkaEx.Cluster.ClusterMetadata{} = metadata
    end

    test "connects with empty extensions" do
      {:ok, opts} = oauth_opts(extensions: %{})
      {:ok, pid} = Client.start_link(opts, :no_name)

      {:ok, metadata} = API.metadata(pid)
      assert %KafkaEx.Cluster.ClusterMetadata{} = metadata
    end
  end

  describe "OAUTHBEARER operations" do
    setup do
      {:ok, opts} = oauth_opts()
      {:ok, pid} = Client.start_link(opts, :no_name)

      {:ok, %{client: pid}}
    end

    test "produces messages", %{client: client} do
      topic_name = generate_random_string()

      {:ok, result} = API.produce(client, topic_name, 0, [%{value: "oauth-msg"}])

      assert result.topic == topic_name
      assert result.base_offset >= 0
    end

    test "fetches messages", %{client: client} do
      topic_name = generate_random_string()

      {:ok, produce_result} = API.produce(client, topic_name, 0, [%{value: "fetch-oauth"}])
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, produce_result.base_offset)

      assert length(fetch_result.records) >= 1
      assert hd(fetch_result.records).value == "fetch-oauth"
    end

    test "retrieves metadata", %{client: client} do
      {:ok, metadata} = API.metadata(client)

      assert %KafkaEx.Cluster.ClusterMetadata{} = metadata
      assert is_map(metadata.brokers)
    end
  end

  describe "OAUTHBEARER token provider" do
    test "calls token provider for each new connection" do
      call_count = :counters.new(1, [:atomics])

      token_provider = fn ->
        :counters.add(call_count, 1, 1)
        {:ok, build_jwt("test-user")}
      end

      {:ok, opts} = oauth_opts(token_provider: token_provider)

      # Create multiple clients
      {:ok, _pid1} = Client.start_link(opts, :no_name)
      {:ok, _pid2} = Client.start_link(opts, :no_name)
      {:ok, _pid3} = Client.start_link(opts, :no_name)

      # Token provider should be called at least 3 times (once per client init)
      assert :counters.get(call_count, 1) >= 3
    end

    test "handles token with different subjects" do
      subjects = ["user-1", "user-2", "service-account"]

      Enum.each(subjects, fn subject ->
        {:ok, opts} = oauth_opts(token: build_jwt(subject))
        {:ok, pid} = Client.start_link(opts, :no_name)

        {:ok, metadata} = API.metadata(pid)
        assert %KafkaEx.Cluster.ClusterMetadata{} = metadata
      end)
    end
  end

  # Helper functions

  defp assert_auth_failure(fun) do
    # Trap exits so linked process crashes don't kill this process
    Process.flag(:trap_exit, true)

    result =
      try do
        fun.()
      catch
        :exit, reason -> {:exit, reason}
      end

    # Clean up any EXIT messages
    receive do
      {:EXIT, _pid, _reason} -> :ok
    after
      100 -> :ok
    end

    Process.flag(:trap_exit, false)

    case result do
      {:error, _} -> :ok
      {:exit, _} -> :ok
      {:ok, _} -> flunk("Expected authentication to fail, but it succeeded")
    end
  end

  defp oauth_opts(overrides \\ []) do
    token = Keyword.get(overrides, :token, build_jwt("test-user"))
    extensions = Keyword.get(overrides, :extensions)

    token_provider =
      Keyword.get(overrides, :token_provider, fn -> {:ok, token} end)

    mechanism_opts =
      %{token_provider: token_provider}
      |> maybe_add_extensions(extensions)

    auth_overrides = [
      uris: [{"localhost", @oauth_port}],
      use_ssl: true,
      ssl_options: [verify: :verify_none],
      auth:
        KafkaEx.Auth.Config.new(%{
          mechanism: :oauthbearer,
          mechanism_opts: mechanism_opts
        })
    ]

    KafkaEx.build_worker_options(auth_overrides)
  end

  defp maybe_add_extensions(opts, nil), do: opts
  defp maybe_add_extensions(opts, extensions), do: Map.put(opts, :extensions, extensions)

  defp build_jwt(subject, opts \\ []) do
    exp_offset = Keyword.get(opts, :exp_offset, 3600)
    header = Base.url_encode64(~s({"alg":"none","typ":"JWT"}), padding: false)
    now = System.system_time(:second)
    payload = Base.url_encode64(~s({"sub":"#{subject}","iat":#{now},"exp":#{now + exp_offset}}), padding: false)

    "#{header}.#{payload}."
  end
end
