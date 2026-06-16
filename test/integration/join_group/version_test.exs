defmodule KafkaEx.Integration.JoinGroup.VersionTest do
  @moduledoc """
  Exercises `JoinGroup` across API versions against the shared integration
  cluster (start it with `./scripts/docker_up.sh`).

  Covers the meaningful protocol boundaries:
    * v3 — last pre-KIP-394 version (one-step join, empty member_id accepted)
    * v4 — first KIP-394 version (two-step join via `member_id_required`)
    * v5 — adds `group_instance_id` (static membership, KIP-345)
    * v6 — first flexible version (KIP-482, compact encoding + tagged fields)

  Each version should return a valid assigned member id and generation. v4+ in
  particular relies on the `:member_id_required` retry being handled correctly
  (see `KafkaEx.Support.Retry.join_group_retryable?/1`).
  """
  use ExUnit.Case, async: false
  @moduletag :consumer_group

  import KafkaEx.TestHelpers
  import KafkaEx.IntegrationHelpers

  alias KafkaEx.API
  alias KafkaEx.Client
  alias KafkaEx.Messages.JoinGroup
  alias KafkaEx.Test.KayrockFixtures, as: Fixtures

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, client} = Client.start_link(args, :no_name)
    on_exit(fn -> if Process.alive?(client), do: GenServer.stop(client) end)
    {:ok, %{client: client}}
  end

  # First join after topic creation can hit a cold __consumer_offsets coordinator
  # (surfaces as :unknown); retry that.
  defp join_with_retry(client, group, topic, version, retries \\ 5) do
    group_protocols = [%{name: "assign", metadata: Fixtures.group_protocol_metadata(topics: [topic])}]

    opts = [
      session_timeout: 30_000,
      rebalance_timeout: 60_000,
      group_protocols: group_protocols,
      api_version: version
    ]

    case API.join_group(client, group, "", opts) do
      {:ok, _} = ok ->
        ok

      {:error, _} when retries > 0 ->
        Process.sleep(1_000)
        join_with_retry(client, group, topic, version, retries - 1)

      {:error, _} = err ->
        err
    end
  end

  for version <- [3, 4, 5, 6] do
    @tag timeout: 120_000
    test "JoinGroup v#{version} returns an assigned member id and generation", %{client: client} do
      topic = generate_random_string()
      group = "cg-join-v#{unquote(version)}-#{generate_random_string()}"
      _ = create_topic(client, topic)

      assert {:ok, %JoinGroup{} = result} = join_with_retry(client, group, topic, unquote(version))

      assert is_binary(result.member_id) and byte_size(result.member_id) > 0
      assert result.generation_id >= 1
      assert JoinGroup.leader?(result)

      # Clean up: a single fresh member is the leader; leaving empties the group.
      {:ok, _} = API.leave_group(client, group, result.member_id)
    end
  end
end
