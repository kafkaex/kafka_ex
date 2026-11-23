defmodule KafkaEx.Client.StateTest do
  use ExUnit.Case, async: true

  alias KafkaEx.Client.State

  describe "static_init/1" do
    test "initializes state with default values" do
      state = State.static_init([])

      assert state.bootstrap_uris == []
      assert state.metadata_update_interval == 30_000
      assert state.allow_auto_topic_creation == true
      assert state.use_ssl == false
      assert state.ssl_options == []
      assert state.auth == nil
      assert state.consumer_group_for_auto_commit == nil
    end

    test "initializes state with custom values" do
      args = [
        uris: [{"localhost", 9092}],
        metadata_update_interval: 60_000,
        allow_auto_topic_creation: false,
        use_ssl: true,
        ssl_options: [verify: :verify_peer],
        auth: {:sasl, :plain, "user", "pass"},
        consumer_group: "my-group"
      ]

      state = State.static_init(args)

      assert state.bootstrap_uris == [{"localhost", 9092}]
      assert state.metadata_update_interval == 60_000
      assert state.allow_auto_topic_creation == false
      assert state.use_ssl == true
      assert state.ssl_options == [verify: :verify_peer]
      assert state.auth == {:sasl, :plain, "user", "pass"}
      assert state.consumer_group_for_auto_commit == "my-group"
    end
  end

  describe "increment_correlation_id/1" do
    test "increments correlation_id by 1" do
      state = %State{correlation_id: 0}

      state = State.increment_correlation_id(state)
      assert state.correlation_id == 1

      state = State.increment_correlation_id(state)
      assert state.correlation_id == 2
    end
  end

  describe "api_version_ranges/1" do
    alias KafkaEx.Messages.ApiVersions

    test "maps each api key to its {min, max} range" do
      api_versions =
        ApiVersions.build(
          api_versions: %{
            0 => %{min_version: 0, max_version: 8},
            18 => %{min_version: 1, max_version: 2}
          }
        )

      assert State.api_version_ranges(api_versions) == %{0 => {0, 8}, 18 => {1, 2}}
    end
  end

  describe "max_supported_api_version/2" do
    test "returns {:ok, version} with min of broker and kayrock max" do
      # Broker supports up to version 10 for produce (api_key 0)
      state = %State{api_versions: %{0 => {0, 10}}}

      # The actual max will be min(broker_max, kayrock_max)
      assert {:ok, version} = State.max_supported_api_version(state, :produce)
      assert is_integer(version)
      assert version <= 10
    end

    test "returns {:error, :api_not_supported_by_broker} when api not in cache" do
      state = %State{api_versions: %{}}

      assert {:error, :api_not_supported_by_broker} = State.max_supported_api_version(state, :produce)
    end

    test "caps at kayrock max when broker reports higher" do
      # Broker reports very high version
      state = %State{api_versions: %{0 => {0, 999}}}

      assert {:ok, version} = State.max_supported_api_version(state, :produce)
      # Should be capped at kayrock's max, which is less than 999
      assert version < 999
    end
  end

  describe "max_supported_api_version/3 (deprecated)" do
    test "returns integer version when api is in cache" do
      state = %State{api_versions: %{0 => {0, 10}}}

      result = apply(State, :max_supported_api_version, [state, :produce, 0])
      assert is_integer(result)
    end

    test "returns default when api not in cache" do
      state = %State{api_versions: %{}}

      result = apply(State, :max_supported_api_version, [state, :produce, 0])
      assert result == 0
    end
  end
end
