defmodule KafkaEx.Chaos.JoinGroupTest do
  @moduledoc """
  Chaos coverage for the consumer-group JOIN path under network latency.

  Regression for the KIP-394 `:member_id_required` retry fix: a v4+ `JoinGroup`
  with an empty `member_id` gets `member_id_required` + a broker-assigned id to
  retry with. The fix makes `member_id_required` non-retryable in the generic
  loop so it bounces straight to the retry-with-assigned-id step (one extra
  round-trip) instead of being blind-retried with the empty id (several wasted
  round-trips). Under latency those wasted round-trips let the assigned id lapse
  and the join never converges.

  This test injects downstream latency tuned so the *fixed* client converges
  (~2 round-trips) while the *unfixed* client would not (~4+ round-trips exceed
  the session window). It is therefore timing-sensitive by nature.
  """
  use ExUnit.Case, async: false
  @moduletag :chaos
  @moduletag timeout: 300_000

  require Logger
  import KafkaEx.TestHelpers

  alias KafkaEx.API
  alias KafkaEx.ChaosTestHelpers
  alias KafkaEx.Messages.JoinGroup
  alias KafkaEx.Test.KayrockFixtures, as: Fixtures

  setup_all do
    {:ok, ctx} = ChaosTestHelpers.start_infrastructure()
    on_exit(fn -> ChaosTestHelpers.stop_infrastructure(ctx) end)
    {:ok, ctx}
  end

  setup ctx do
    ChaosTestHelpers.reset_all(ctx.toxiproxy_container)
    ChaosTestHelpers.stop_client()
    Process.sleep(200)

    on_exit(fn ->
      ChaosTestHelpers.reset_all(ctx.toxiproxy_container)
      ChaosTestHelpers.stop_client()
    end)

    {:ok, ctx}
  end

  defp join(client, group, version, session_timeout) do
    protocols = [%{name: "assign", metadata: Fixtures.group_protocol_metadata(topics: ["t-#{group}"])}]

    API.join_group(client, group, "",
      session_timeout: session_timeout,
      rebalance_timeout: 60_000,
      group_protocols: protocols,
      api_version: version
    )
  end

  # Retried join — gets past a cold __consumer_offsets coordinator (which also
  # surfaces as :unknown) so the warm-up is a clean signal.
  defp join_retry(client, group, version, session_timeout, retries \\ 6) do
    case join(client, group, version, session_timeout) do
      {:ok, _} = ok ->
        ok

      {:error, _} when retries > 0 ->
        Process.sleep(1_000)
        join_retry(client, group, version, session_timeout, retries - 1)

      err ->
        err
    end
  end

  # v6 is the flexible version; the KIP-394 two-step join behaves identically for all v4+.
  for version <- [6] do
    test "JoinGroup v#{version} converges under network latency (KIP-394 member_id_required handled in one extra round-trip)",
         ctx do
      {:ok, client} = ChaosTestHelpers.start_client(ctx)

      # Warm up the coordinator so the latency join below isn't flaky on a cold start.
      _ = join_retry(client, "joingroup-warmup-#{generate_random_string()}", unquote(version), 30_000)

      # session_timeout 6000 = broker's group.min.session.timeout.ms floor.
      # At 2000ms latency the fixed client (~2 round-trips ≈ 4s) converges within
      # the 6s session; the unfixed client (~4+ round-trips ≈ 8s+) would not.
      result =
        ChaosTestHelpers.with_latency(ctx.toxiproxy_container, ctx.proxy_name, 2_000, fn ->
          join(client, "joingroup-latency-#{generate_random_string()}", unquote(version), 6_000)
        end)

      Logger.warning("[+2000ms latency, 6s session] JoinGroup v#{unquote(version)} => #{inspect(result)}")
      assert {:ok, %JoinGroup{}} = result
    end
  end
end
