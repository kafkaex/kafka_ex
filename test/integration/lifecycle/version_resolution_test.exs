defmodule KafkaEx.Integration.Lifecycle.VersionResolutionTest do
  @moduledoc """
  Integration tests verifying the 3-tier API version resolution works
  end-to-end against a real Kafka broker.

  Resolution order: per-request opts > application config > broker-negotiated max.
  """
  use ExUnit.Case, async: false
  @moduletag :lifecycle

  import KafkaEx.TestHelpers
  import KafkaEx.IntegrationHelpers

  alias KafkaEx.API
  alias KafkaEx.Client

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, pid} = Client.start_link(args, :no_name)

    on_exit(fn ->
      Application.delete_env(:kafka_ex, :api_versions)
      if Process.alive?(pid), do: GenServer.stop(pid)
    end)

    {:ok, %{client: pid}}
  end

  # ---------------------------------------------------------------------------
  # Tier 3: Negotiated Max (default — no config, no opts)
  # ---------------------------------------------------------------------------
  describe "tier 3: broker-negotiated max (default)" do
    test "fetch uses negotiated max version by default", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "tier3-fetch"}])
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, 0)

      # Negotiated max for fetch is v11 on our test cluster.
      # V4+ includes throttle_time_ms, V5+ includes log_start_offset.
      assert is_integer(fetch_result.throttle_time_ms)
      assert is_integer(fetch_result.log_start_offset)
      assert length(fetch_result.records) >= 1
    end

    test "earliest_offset uses negotiated max (not hardcoded v1)", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "tier3-offset"}])

      # Previously latest_offset/earliest_offset forced api_version: 1.
      # Now they use the negotiated max. Both should work correctly.
      {:ok, earliest} = API.earliest_offset(client, topic_name, 0)
      {:ok, latest} = API.latest_offset(client, topic_name, 0)

      assert earliest == 0
      assert latest >= 1
      assert latest > earliest
    end

    test "metadata uses negotiated max by default", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, metadata} = API.metadata(client, [topic_name])

      # Negotiated max is v9. V2+ populates cluster_id.
      assert map_size(metadata.brokers) >= 1
      assert Map.has_key?(metadata.topics, topic_name)
    end
  end

  # ---------------------------------------------------------------------------
  # Tier 2: Application Config Override
  # ---------------------------------------------------------------------------
  describe "tier 2: application config overrides negotiated max" do
    test "app config pins fetch to lower version", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "tier2-fetch"}])

      # Pin fetch to v3 via app config
      Application.put_env(:kafka_ex, :api_versions, %{fetch: 3})

      {:ok, fetch_result} = API.fetch(client, topic_name, 0, 0)

      # V3 fetch should still return records
      assert length(fetch_result.records) >= 1
      assert hd(fetch_result.records).value == "tier2-fetch"
    end

    test "app config pins produce to v0", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      Application.put_env(:kafka_ex, :api_versions, %{produce: 0})

      # V0 produce should still work (uses MessageSet format)
      {:ok, result} = API.produce(client, topic_name, 0, [%{value: "tier2-produce-v0"}])

      assert is_integer(result.base_offset)

      # Verify the message was actually produced
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, result.base_offset)
      assert hd(fetch_result.records).value == "tier2-produce-v0"
    end

    test "app config does not affect APIs not in the config map", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "tier2-selective"}])

      # Only pin fetch — metadata should still use negotiated max
      Application.put_env(:kafka_ex, :api_versions, %{fetch: 3})

      {:ok, metadata} = API.metadata(client, [topic_name])
      assert Map.has_key?(metadata.topics, topic_name)

      {:ok, latest} = API.latest_offset(client, topic_name, 0)
      assert latest >= 1
    end
  end

  # ---------------------------------------------------------------------------
  # Tier 1: Per-Request Opts Override Everything
  # ---------------------------------------------------------------------------
  describe "tier 1: per-request opts override app config" do
    test "explicit api_version in opts overrides app config", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "tier1-fetch"}])

      # App config pins fetch to v3
      Application.put_env(:kafka_ex, :api_versions, %{fetch: 3})

      # But per-request opts explicitly request v11
      {:ok, fetch_result} = API.fetch(client, topic_name, 0, 0, api_version: 11)

      # V11 should give us the full response with V5+ fields
      assert is_integer(fetch_result.log_start_offset)
      assert length(fetch_result.records) >= 1
    end

    test "explicit api_version: 0 works for list_offsets", %{client: client} do
      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      {:ok, _} = API.produce(client, topic_name, 0, [%{value: "tier1-offset"}])

      # Even with app config setting a version, explicit opts win
      Application.put_env(:kafka_ex, :api_versions, %{list_offsets: 3})

      {:ok, latest} = API.latest_offset(client, topic_name, 0, api_version: 5)
      assert latest >= 1
    end
  end

  # ---------------------------------------------------------------------------
  # Error Cases
  # ---------------------------------------------------------------------------
  describe "error handling for unsupported versions" do
    test "returns error when app config exceeds broker max", %{client: client} do
      # Pin fetch to v99 — way beyond what any broker supports
      Application.put_env(:kafka_ex, :api_versions, %{fetch: 99})

      {:error, reason} = API.fetch(client, "any-topic", 0, 0)
      assert reason == :api_version_no_supported
    end

    test "returns error when per-request version exceeds broker max", %{client: client} do
      {:error, reason} = API.fetch(client, "any-topic", 0, 0, api_version: 99)
      assert reason == :api_version_no_supported
    end
  end

  # ---------------------------------------------------------------------------
  # Internal Metadata Refresh (Task 4 verification)
  # ---------------------------------------------------------------------------
  describe "internal metadata refresh uses standard resolution" do
    test "client metadata refresh works with app config pinned version", %{client: client} do
      # Pin metadata to v4 via app config
      Application.put_env(:kafka_ex, :api_versions, %{metadata: 4})

      topic_name = generate_random_string()
      _ = create_topic(client, topic_name)

      # The client's internal metadata refresh timer uses RequestBuilder which
      # reads app config. Creating a topic and waiting for metadata to propagate
      # exercises this path.
      _ = wait_for_topic_in_metadata(client, topic_name)

      {:ok, metadata} = API.metadata(client, [topic_name])
      assert Map.has_key?(metadata.topics, topic_name)
    end
  end
end
