defmodule KafkaEx.Integration.Auth.PlainTest do
  use ExUnit.Case
  @moduletag :auth

  import KafkaEx.TestHelpers

  alias KafkaEx.API
  alias KafkaEx.Client

  @plain_port 9192

  describe "SASL/PLAIN connection" do
    test "connects successfully with valid credentials" do
      {:ok, opts} = plain_opts()
      {:ok, pid} = Client.start_link(opts, :no_name)

      assert Process.alive?(pid)

      {:ok, metadata} = API.metadata(pid)
      assert %KafkaEx.Cluster.ClusterMetadata{} = metadata
      assert map_size(metadata.brokers) >= 1
    end

    test "fails with invalid username" do
      {:ok, opts} = plain_opts(username: "wrong_user")

      assert_auth_failure(fn -> Client.start_link(opts, :no_name) end)
    end

    test "fails with invalid password" do
      {:ok, opts} = plain_opts(password: "wrong_password")

      assert_auth_failure(fn -> Client.start_link(opts, :no_name) end)
    end

    test "fails with empty credentials" do
      {:ok, opts} = plain_opts(username: "", password: "")

      assert_auth_failure(fn -> Client.start_link(opts, :no_name) end)
    end
  end

  describe "SASL/PLAIN operations" do
    setup do
      {:ok, opts} = plain_opts()
      {:ok, pid} = Client.start_link(opts, :no_name)

      {:ok, %{client: pid}}
    end

    test "produces messages with PLAIN auth", %{client: client} do
      topic_name = generate_random_string()

      {:ok, result} = API.produce(client, topic_name, 0, [%{value: "plain-auth-msg"}])

      assert result.topic == topic_name
      assert result.partition == 0
      assert result.base_offset >= 0
    end

    test "fetches messages with PLAIN auth", %{client: client} do
      topic_name = generate_random_string()

      {:ok, produce_result} = API.produce(client, topic_name, 0, [%{value: "fetch-with-plain"}])

      {:ok, fetch_result} = API.fetch(client, topic_name, 0, produce_result.base_offset)

      assert length(fetch_result.records) >= 1
      assert hd(fetch_result.records).value == "fetch-with-plain"
    end

    test "retrieves metadata with PLAIN auth", %{client: client} do
      {:ok, metadata} = API.metadata(client)

      assert %KafkaEx.Cluster.ClusterMetadata{} = metadata
      assert is_map(metadata.brokers)
      assert is_map(metadata.topics)
    end

    test "retrieves offsets with PLAIN auth", %{client: client} do
      topic_name = generate_random_string()
      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "offset-test"}])

      {:ok, earliest} = API.earliest_offset(client, topic_name, 0)
      {:ok, latest} = API.latest_offset(client, topic_name, 0)

      assert earliest >= 0
      assert latest >= earliest
    end
  end

  describe "SASL/PLAIN reconnection" do
    test "maintains auth after metadata refresh" do
      {:ok, opts} = plain_opts()
      {:ok, pid} = Client.start_link(opts, :no_name)

      {:ok, metadata1} = API.metadata(pid)

      # Force metadata refresh
      Process.sleep(100)
      {:ok, metadata2} = API.metadata(pid)

      assert %KafkaEx.Cluster.ClusterMetadata{} = metadata1
      assert %KafkaEx.Cluster.ClusterMetadata{} = metadata2
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

  defp plain_opts(overrides \\ []) do
    username = Keyword.get(overrides, :username, "test")
    password = Keyword.get(overrides, :password, "secret")

    auth_overrides = [
      uris: [{"localhost", @plain_port}],
      use_ssl: true,
      ssl_options: [verify: :verify_none],
      auth:
        KafkaEx.Auth.Config.new(%{
          mechanism: :plain,
          username: username,
          password: password
        })
    ]

    KafkaEx.build_worker_options(auth_overrides)
  end
end
