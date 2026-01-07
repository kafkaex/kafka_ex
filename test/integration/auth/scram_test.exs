defmodule KafkaEx.Integration.Auth.ScramTest do
  use ExUnit.Case
  @moduletag :auth

  import KafkaEx.TestHelpers

  alias KafkaEx.API
  alias KafkaEx.Client

  @scram_port 9292

  describe "SCRAM-SHA-256 connection" do
    test "connects successfully with valid credentials" do
      {:ok, opts} = scram_opts(:sha256)
      {:ok, pid} = Client.start_link(opts, :no_name)

      assert Process.alive?(pid)

      {:ok, metadata} = API.metadata(pid)
      assert %KafkaEx.Cluster.ClusterMetadata{} = metadata
    end

    test "fails with invalid password" do
      {:ok, opts} = scram_opts(:sha256, password: "wrong_password")

      assert_auth_failure(fn -> Client.start_link(opts, :no_name) end)
    end

    test "fails with invalid username" do
      {:ok, opts} = scram_opts(:sha256, username: "nonexistent")

      assert_auth_failure(fn -> Client.start_link(opts, :no_name) end)
    end
  end

  describe "SCRAM-SHA-512 connection" do
    test "connects successfully with valid credentials" do
      {:ok, opts} = scram_opts(:sha512)
      {:ok, pid} = Client.start_link(opts, :no_name)

      assert Process.alive?(pid)

      {:ok, metadata} = API.metadata(pid)
      assert %KafkaEx.Cluster.ClusterMetadata{} = metadata
    end

    test "fails with invalid password" do
      {:ok, opts} = scram_opts(:sha512, password: "wrong_password")

      assert_auth_failure(fn -> Client.start_link(opts, :no_name) end)
    end
  end

  describe "SCRAM-SHA-256 operations" do
    setup do
      {:ok, opts} = scram_opts(:sha256)
      {:ok, pid} = Client.start_link(opts, :no_name)

      {:ok, %{client: pid}}
    end

    test "produces messages", %{client: client} do
      topic_name = generate_random_string()

      {:ok, result} = API.produce(client, topic_name, 0, [%{value: "scram-256-msg"}])

      assert result.topic == topic_name
      assert result.base_offset >= 0
    end

    test "fetches messages", %{client: client} do
      topic_name = generate_random_string()

      {:ok, produce_result} = API.produce(client, topic_name, 0, [%{value: "fetch-scram-256"}])
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, produce_result.base_offset)

      assert length(fetch_result.records) >= 1
      assert hd(fetch_result.records).value == "fetch-scram-256"
    end

    test "retrieves metadata", %{client: client} do
      {:ok, metadata} = API.metadata(client)

      assert %KafkaEx.Cluster.ClusterMetadata{} = metadata
      assert is_map(metadata.brokers)
    end
  end

  describe "SCRAM-SHA-512 operations" do
    setup do
      {:ok, opts} = scram_opts(:sha512)
      {:ok, pid} = Client.start_link(opts, :no_name)

      {:ok, %{client: pid}}
    end

    test "produces messages", %{client: client} do
      topic_name = generate_random_string()

      {:ok, result} = API.produce(client, topic_name, 0, [%{value: "scram-512-msg"}])

      assert result.topic == topic_name
      assert result.base_offset >= 0
    end

    test "fetches messages", %{client: client} do
      topic_name = generate_random_string()

      {:ok, produce_result} = API.produce(client, topic_name, 0, [%{value: "fetch-scram-512"}])
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, produce_result.base_offset)

      assert length(fetch_result.records) >= 1
      assert hd(fetch_result.records).value == "fetch-scram-512"
    end
  end

  describe "SCRAM challenge-response" do
    test "handles multiple sequential connections with SHA-256" do
      {:ok, opts} = scram_opts(:sha256)

      # Create multiple clients sequentially
      clients =
        Enum.map(1..3, fn _ ->
          {:ok, pid} = Client.start_link(opts, :no_name)
          pid
        end)

      # All should work
      Enum.each(clients, fn client ->
        {:ok, metadata} = API.metadata(client)
        assert %KafkaEx.Cluster.ClusterMetadata{} = metadata
      end)
    end

    test "handles multiple sequential connections with SHA-512" do
      {:ok, opts} = scram_opts(:sha512)

      clients =
        Enum.map(1..3, fn _ ->
          {:ok, pid} = Client.start_link(opts, :no_name)
          pid
        end)

      Enum.each(clients, fn client ->
        {:ok, metadata} = API.metadata(client)
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

  defp scram_opts(algo, overrides \\ []) do
    username = Keyword.get(overrides, :username, "test")
    password = Keyword.get(overrides, :password, "secret")

    auth_overrides = [
      uris: [{"localhost", @scram_port}],
      use_ssl: true,
      ssl_options: [verify: :verify_none],
      auth:
        KafkaEx.Auth.Config.new(%{
          mechanism: :scram,
          username: username,
          password: password,
          mechanism_opts: %{algo: algo}
        })
    ]

    KafkaEx.build_worker_options(auth_overrides)
  end
end
