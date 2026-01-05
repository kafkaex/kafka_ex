defmodule KafkaEx.Integration.Auth.SslTest do
  use ExUnit.Case
  @moduletag :auth

  import KafkaEx.TestHelpers

  alias KafkaEx.API
  alias KafkaEx.Client

  @ssl_port 9092

  describe "SSL connection without SASL" do
    test "connects successfully with SSL" do
      {:ok, opts} = ssl_opts()
      {:ok, pid} = Client.start_link(opts, :no_name)

      assert Process.alive?(pid)

      {:ok, metadata} = API.metadata(pid)
      assert %KafkaEx.Cluster.ClusterMetadata{} = metadata
    end

    test "retrieves cluster metadata over SSL" do
      {:ok, opts} = ssl_opts()
      {:ok, pid} = Client.start_link(opts, :no_name)

      {:ok, metadata} = API.metadata(pid)

      assert %KafkaEx.Cluster.ClusterMetadata{} = metadata
      assert map_size(metadata.brokers) >= 1
      assert is_map(metadata.topics)
    end
  end

  describe "SSL operations" do
    setup do
      {:ok, opts} = ssl_opts()
      {:ok, pid} = Client.start_link(opts, :no_name)

      {:ok, %{client: pid}}
    end

    test "produces messages over SSL", %{client: client} do
      topic_name = generate_random_string()

      {:ok, result} = API.produce(client, topic_name, 0, [%{value: "ssl-message"}])

      assert result.topic == topic_name
      assert result.base_offset >= 0
    end

    test "fetches messages over SSL", %{client: client} do
      topic_name = generate_random_string()

      {:ok, produce_result} = API.produce(client, topic_name, 0, [%{value: "fetch-ssl"}])
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, produce_result.base_offset)

      assert length(fetch_result.records) >= 1
      assert hd(fetch_result.records).value == "fetch-ssl"
    end

    test "retrieves offsets over SSL", %{client: client} do
      topic_name = generate_random_string()
      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "offset-ssl"}])

      {:ok, earliest} = API.earliest_offset(client, topic_name, 0)
      {:ok, latest} = API.latest_offset(client, topic_name, 0)

      assert earliest >= 0
      assert latest > earliest
    end

    test "batch produce over SSL", %{client: client} do
      topic_name = generate_random_string()
      messages = Enum.map(1..100, fn i -> %{value: "ssl-batch-#{i}"} end)

      {:ok, result} = API.produce(client, topic_name, 0, messages)

      assert result.topic == topic_name
      assert result.base_offset >= 0

      {:ok, latest} = API.latest_offset(client, topic_name, 0)
      assert latest >= result.base_offset + 100
    end
  end

  describe "SSL with multiple brokers" do
    test "connects to multiple SSL brokers" do
      # All 3 brokers use SSL on ports 9092-9094
      {:ok, opts} =
        KafkaEx.build_worker_options(
          uris: [{"localhost", 9092}, {"localhost", 9093}, {"localhost", 9094}],
          use_ssl: true,
          ssl_options: [verify: :verify_none]
        )

      {:ok, pid} = Client.start_link(opts, :no_name)

      {:ok, metadata} = API.metadata(pid)
      assert map_size(metadata.brokers) >= 1
    end
  end

  # Helper functions

  defp ssl_opts do
    KafkaEx.build_worker_options(
      uris: [{"localhost", @ssl_port}],
      use_ssl: true,
      ssl_options: [verify: :verify_none]
    )
  end
end
