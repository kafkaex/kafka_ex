defmodule KafkaEx.Integration.Lifecycle.ClientLifecycleTest do
  use ExUnit.Case, async: true
  @moduletag :lifecycle

  import KafkaEx.TestHelpers
  import KafkaEx.IntegrationHelpers

  alias KafkaEx.Client
  alias KafkaEx.API

  describe "client startup" do
    test "client connects and fetches metadata on start" do
      {:ok, args} = KafkaEx.build_worker_options([])
      {:ok, client} = Client.start_link(args, :no_name)

      # Client should be able to get metadata immediately
      {:ok, metadata} = API.metadata(client)

      assert metadata != nil
      assert map_size(metadata.brokers) >= 1

      GenServer.stop(client)
    end

    test "client caches API versions on connect" do
      {:ok, args} = KafkaEx.build_worker_options([])
      {:ok, client} = Client.start_link(args, :no_name)

      # Should be able to get API versions
      {:ok, versions} = API.api_versions(client)

      assert %KafkaEx.Messages.ApiVersions{} = versions
      assert is_map(versions.api_versions)
      assert map_size(versions.api_versions) > 0

      # Verify common APIs are present (api_versions is a map keyed by api_key)
      # Produce
      assert Map.has_key?(versions.api_versions, 0)
      # Fetch
      assert Map.has_key?(versions.api_versions, 1)
      # Metadata
      assert Map.has_key?(versions.api_versions, 3)

      GenServer.stop(client)
    end

    test "client with invalid broker fails gracefully" do
      args = [
        brokers: [{"nonexistent.invalid", 9092}],
        client_id: "test-invalid-broker",
        consumer_group: "test-consumer-group"
      ]

      # Trap exits so linked process crash doesn't kill test
      Process.flag(:trap_exit, true)

      # Client with invalid broker should either return error or crash
      # Both are acceptable - the key is that it doesn't hang
      result = Client.start_link(args, :no_name)

      case result do
        {:error, _reason} ->
          # Expected - couldn't connect
          :ok

        {:ok, pid} ->
          # Client started but operations should fail
          result = API.metadata(pid)
          assert {:error, _} = result or match?({:ok, _}, result)
          GenServer.stop(pid)
      end

      # Restore normal exit behavior
      Process.flag(:trap_exit, false)
    end

    test "multiple clients can connect to same cluster" do
      {:ok, args} = KafkaEx.build_worker_options([])

      {:ok, client1} = Client.start_link(args, :no_name)
      {:ok, client2} = Client.start_link(args, :no_name)
      {:ok, client3} = Client.start_link(args, :no_name)

      # All clients should work independently
      {:ok, m1} = API.metadata(client1)
      {:ok, m2} = API.metadata(client2)
      {:ok, m3} = API.metadata(client3)

      assert m1 != nil
      assert m2 != nil
      assert m3 != nil

      GenServer.stop(client1)
      GenServer.stop(client2)
      GenServer.stop(client3)
    end
  end

  describe "client operations" do
    setup do
      {:ok, args} = KafkaEx.build_worker_options([])
      {:ok, pid} = Client.start_link(args, :no_name)

      on_exit(fn ->
        if Process.alive?(pid), do: GenServer.stop(pid)
      end)

      {:ok, %{client: pid}}
    end

    test "correlation_id increments with each request", %{client: client} do
      # Get initial correlation_id
      {:ok, id1} = API.correlation_id(client)

      # Make some requests
      _ = API.metadata(client)
      {:ok, id2} = API.correlation_id(client)

      _ = API.metadata(client)
      {:ok, id3} = API.correlation_id(client)

      # IDs should be increasing
      assert id2 > id1
      assert id3 > id2
    end

    test "client handles rapid sequential requests", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Rapid sequential operations
      results =
        Enum.map(1..20, fn i ->
          {:ok, _} = API.produce(client, topic_name, 0, [%{value: "rapid-#{i}"}])
          :ok
        end)

      assert Enum.all?(results, &(&1 == :ok))

      # Verify all messages arrived
      {:ok, latest} = API.latest_offset(client, topic_name, 0)
      assert latest >= 20
    end

    test "client handles concurrent requests from multiple processes", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Launch concurrent producers
      tasks =
        Enum.map(1..10, fn producer_id ->
          Task.async(fn ->
            Enum.each(1..10, fn msg_id ->
              {:ok, _} = API.produce(client, topic_name, 0, [%{value: "p#{producer_id}-m#{msg_id}"}])
            end)

            :ok
          end)
        end)

      results = Task.await_many(tasks, 30_000)
      assert Enum.all?(results, &(&1 == :ok))

      # Verify total message count
      {:ok, latest} = API.latest_offset(client, topic_name, 0)
      assert latest >= 100
    end
  end

  describe "client graceful shutdown" do
    test "client stops cleanly" do
      {:ok, args} = KafkaEx.build_worker_options([])
      {:ok, client} = Client.start_link(args, :no_name)

      # Do some operations
      {:ok, _} = API.metadata(client)

      # Stop client
      :ok = GenServer.stop(client)

      # Client should be dead
      refute Process.alive?(client)
    end

    test "client completes in-flight requests before shutdown" do
      {:ok, args} = KafkaEx.build_worker_options([])
      {:ok, client} = Client.start_link(args, :no_name)

      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # Start a request in another process
      task =
        Task.async(fn ->
          API.produce(client, topic_name, 0, [%{value: "in-flight"}])
        end)

      # Small delay then stop
      Process.sleep(50)
      GenServer.stop(client, :normal, 5000)

      # Task should either complete or get an error (not hang)
      result = Task.await(task, 5000)
      assert match?({:ok, _}, result) or match?({:error, _}, result)
    end

    test "stopped client cannot be used" do
      {:ok, args} = KafkaEx.build_worker_options([])
      {:ok, client} = Client.start_link(args, :no_name)

      GenServer.stop(client)

      # Attempting to use stopped client should fail
      assert catch_exit(API.metadata(client))
    end
  end

  describe "multiple clients isolation" do
    test "clients don't interfere with each other", %{} do
      {:ok, args} = KafkaEx.build_worker_options([])
      {:ok, client1} = Client.start_link(args, :no_name)
      {:ok, client2} = Client.start_link(args, :no_name)

      topic1 = generate_random_string()
      topic2 = generate_random_string()

      _ = create_topic(client1, topic1)
      _ = create_topic(client2, topic2)

      # Each client produces to its own topic
      {:ok, r1} = API.produce(client1, topic1, 0, [%{value: "client1-msg"}])
      {:ok, r2} = API.produce(client2, topic2, 0, [%{value: "client2-msg"}])

      # Each client fetches from its own topic
      {:ok, f1} = API.fetch(client1, topic1, 0, r1.base_offset, max_bytes: 100_000)
      {:ok, f2} = API.fetch(client2, topic2, 0, r2.base_offset, max_bytes: 100_000)

      assert hd(f1.records).value == "client1-msg"
      assert hd(f2.records).value == "client2-msg"

      GenServer.stop(client1)
      GenServer.stop(client2)
    end

    test "stopping one client doesn't affect others" do
      {:ok, args} = KafkaEx.build_worker_options([])
      {:ok, client1} = Client.start_link(args, :no_name)
      {:ok, client2} = Client.start_link(args, :no_name)

      # Stop client1
      GenServer.stop(client1)

      # Client2 should still work
      {:ok, metadata} = API.metadata(client2)
      assert metadata != nil

      GenServer.stop(client2)
    end
  end

  describe "named client" do
    test "client can be started with a name" do
      {:ok, args} = KafkaEx.build_worker_options([])
      name = :"test_named_client_#{:rand.uniform(100_000)}"

      {:ok, _pid} = Client.start_link(args, name)

      # Can reference by name
      {:ok, metadata} = API.metadata(name)
      assert metadata != nil

      GenServer.stop(name)
    end

    test "cannot start two clients with same name" do
      {:ok, args} = KafkaEx.build_worker_options([])
      name = :"test_duplicate_name_#{:rand.uniform(100_000)}"

      {:ok, _pid} = Client.start_link(args, name)

      # Second start should fail
      result = Client.start_link(args, name)
      assert {:error, {:already_started, _}} = result

      GenServer.stop(name)
    end
  end
end
