defmodule KafkaEx.Consumer.ConsumerGroupProtocolOptionTest do
  @moduledoc """
  `KafkaEx.Consumer.ConsumerGroup.start_link/4`'s `:group_protocol` option:
  validation, which manager each value starts, the single warning about
  options the selected protocol ignores, and the fail-fast start against a
  cluster without the KIP-848 consumer group protocol.

  The client is a stub that answers ApiVersions as scripted and forwards
  ConsumerGroupHeartbeat calls to the test process.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias KafkaEx.Consumer.ConsumerGroup
  alias KafkaEx.Consumer.ConsumerGroup.ConsumerProtocolManager
  alias KafkaEx.Consumer.ConsumerGroup.Manager
  alias KafkaEx.Messages.ApiVersions

  @group "group-protocol-option"
  # ConsumerGroupHeartbeat
  @heartbeat_api_key 68

  defmodule StubClient do
    @moduledoc false
    use GenServer

    def start_link(args), do: GenServer.start_link(__MODULE__, args)
    def init(args), do: {:ok, args}

    def handle_call({:api_versions, _opts}, _from, %{api_versions: reply} = state), do: {:reply, reply, state}

    def handle_call({:consumer_group_heartbeat, group, opts}, from, %{test_pid: test_pid} = state) do
      send(test_pid, {:heartbeat_call, from, group, opts})
      {:noreply, state}
    end

    def handle_call(other, _from, state), do: {:reply, {:error, {:unexpected, other}}, state}
  end

  # Dies while answering ApiVersions, so the caller's GenServer.call exits
  # (as it would when the client crashes mid-call).
  defmodule DyingClient do
    @moduledoc false
    use GenServer

    def start_link(arg), do: GenServer.start_link(__MODULE__, arg)
    def init(arg), do: {:ok, arg}

    def handle_call({:api_versions, _opts}, _from, state), do: {:stop, :client_crashed, state}
  end

  defmodule NoopConsumer do
    @moduledoc false
  end

  defp api_versions(keys) do
    {:ok, %ApiVersions{api_versions: Map.new(keys, &{&1, %{min_version: 0, max_version: 1}})}}
  end

  defp with_heartbeat_api, do: api_versions([3, 8, 9, 10, @heartbeat_api_key])
  defp without_heartbeat_api, do: api_versions([3, 8, 9, 10, 11, 12, 13, 14])

  defp start_stub(reply) do
    {:ok, client} = StubClient.start_link(%{api_versions: reply, test_pid: self()})
    Process.unlink(client)
    on_exit(fn -> Process.exit(client, :kill) end)
    client
  end

  defp start_group(opts) do
    case ConsumerGroup.start_link({KafkaEx.Consumer.GenConsumer, NoopConsumer}, @group, ["topic"], opts) do
      {:ok, sup} ->
        Process.unlink(sup)
        on_exit(fn -> stop(sup) end)
        {:ok, sup}

      other ->
        other
    end
  end

  defp stop(sup) do
    Supervisor.stop(sup, :shutdown, 10_000)
  catch
    :exit, _ -> Process.exit(sup, :kill)
  end

  defp child_module(sup) do
    [{id, _pid, _, _} | _] = Supervisor.which_children(sup) |> Enum.reverse()
    id
  end

  describe "validation" do
    for value <- [:kip848, "consumer", nil, 1, :Classic] do
      test "#{inspect(value)} is rejected with a clear error before anything starts" do
        value = unquote(value)

        log =
          capture_log(fn ->
            assert {:error, {:invalid_option, :invalid_group_protocol}} = start_group(group_protocol: value)
          end)

        assert log =~ "Invalid :group_protocol #{inspect(value)}"
      end
    end
  end

  describe "selection" do
    test ":classic is the default and starts the classic Manager" do
      assert {:ok, {_flags, [child]}} =
               ConsumerGroup.init({{KafkaEx.Consumer.GenConsumer, NoopConsumer}, @group, ["topic"], []})

      assert child.id == Manager
    end

    test ":classic builds the pre-option child spec: Manager id, start MFA, 25s shutdown, options as given" do
      # Pinned to fixed values (the child spec before group_protocol existed),
      # so a change to the classic branch of init/1 cannot pass unnoticed.
      opts = [heartbeat_interval: 1_000, session_timeout: 30_000, uris: [{"localhost", 9092}], extra: :kept]

      for input <- [opts, opts ++ [group_protocol: :classic]] do
        assert {:ok, {flags, [child]}} =
                 ConsumerGroup.init({{KafkaEx.Consumer.GenConsumer, NoopConsumer}, @group, ["topic"], input})

        # Elixir 1.15+ also adds auto_shutdown: :never; 1.14 does not. Pin the
        # flags that matter and allow only that version-dependent default.
        assert Map.take(flags, [:strategy, :intensity, :period]) == %{strategy: :one_for_all, intensity: 0, period: 1}
        assert Map.drop(flags, [:strategy, :intensity, :period]) in [%{}, %{auto_shutdown: :never}]

        assert %{
                 id: Manager,
                 start:
                   {Manager, :start_link, [{{KafkaEx.Consumer.GenConsumer, NoopConsumer}, @group, ["topic"], passed}]},
                 shutdown: 25_000
               } = child

        assert child == %{id: Manager, start: child.start, shutdown: 25_000}
        # Keyword.put, as before the option: supervisor_pid first, then the input.
        assert passed == [supervisor_pid: self()] ++ opts
      end
    end

    test "explicit group_protocol: :classic builds exactly the default child spec" do
      args = fn opts -> {{KafkaEx.Consumer.GenConsumer, NoopConsumer}, @group, ["topic"], opts} end
      {:ok, {flags, [default]}} = ConsumerGroup.init(args.(heartbeat_interval: 1_000))
      {:ok, {^flags, [explicit]}} = ConsumerGroup.init(args.(heartbeat_interval: 1_000, group_protocol: :classic))

      # The option itself is not passed on: the classic Manager (and its
      # consumers) see exactly the options they saw before it existed.
      {Manager, :start_link, [{_, _, _, default_opts}]} = default.start
      {Manager, :start_link, [{_, _, _, explicit_opts}]} = explicit.start
      assert Keyword.delete(default_opts, :supervisor_pid) == Keyword.delete(explicit_opts, :supervisor_pid)
      refute Keyword.has_key?(explicit_opts, :group_protocol)
      assert Map.delete(default, :start) == Map.delete(explicit, :start)
    end

    test ":consumer starts ConsumerProtocolManager, which joins with the consumer protocol" do
      client = start_stub(with_heartbeat_api())

      capture_log(fn ->
        {:ok, sup} = start_group(client: client, group_protocol: :consumer)
        assert child_module(sup) == ConsumerProtocolManager
        assert ConsumerGroup.group_protocol(sup) == :consumer
        assert_receive {:heartbeat_call, _from, @group, opts}, 2_000
        assert opts[:member_epoch] == 0
        assert opts[:subscribed_topic_names] == ["topic"]
      end)
    end

    test "group_protocol/1 reports :classic for a classic group" do
      {:ok, {_flags, [child]}} =
        ConsumerGroup.init({{KafkaEx.Consumer.GenConsumer, NoopConsumer}, @group, ["topic"], []})

      assert child.id == Manager

      # A supervisor whose only child is a Manager stand-in.
      {:ok, sup} =
        Supervisor.start_link([%{id: Manager, start: {Agent, :start_link, [fn -> nil end]}}], strategy: :one_for_one)

      assert ConsumerGroup.group_protocol(sup) == :classic
      Supervisor.stop(sup)
    end
  end

  describe "ignored options" do
    test "classic-only options under :consumer: one warning listing them, the group still starts" do
      client = start_stub(with_heartbeat_api())

      log =
        capture_log(fn ->
          {:ok, sup} =
            start_group(
              client: client,
              group_protocol: :consumer,
              heartbeat_interval: 1_000,
              session_timeout: 30_000,
              session_timeout_padding: 5_000,
              rebalance_timeout: 60_000,
              partition_assignment_callback: fn members, _ -> Map.new(members, &{&1, []}) end
            )

          assert Process.alive?(sup)
          assert_receive {:heartbeat_call, _from, @group, _opts}, 2_000
        end)

      assert [warning] = Regex.scan(~r/Consumer group [^\n]*ignores the options [^\n]*/, log)
      [warning] = warning
      assert warning =~ ~s("#{@group}")
      assert warning =~ "group_protocol: :consumer"

      for key <- [:heartbeat_interval, :session_timeout, :session_timeout_padding, :rebalance_timeout] do
        assert warning =~ inspect(key)
      end

      assert warning =~ ":partition_assignment_callback"
      assert warning =~ "rebalance_timeout_ms"
    end

    test "classic-only options under :consumer are not passed on to the manager or its consumers" do
      {:ok, {_flags, [child]}} =
        ConsumerGroup.init(
          {{KafkaEx.Consumer.GenConsumer, NoopConsumer}, @group, ["topic"],
           [group_protocol: :consumer, heartbeat_interval: 1_000, session_timeout: 30_000, commit_interval: 10]}
        )

      {ConsumerProtocolManager, :start_link, [{_, _, _, opts}]} = child.start
      refute Keyword.has_key?(opts, :heartbeat_interval)
      refute Keyword.has_key?(opts, :session_timeout)
      refute Keyword.has_key?(opts, :group_protocol)
      assert opts[:commit_interval] == 10
    end

    test "consumer-only options under :classic: one warning listing them" do
      log =
        capture_log(fn ->
          assert {:ok, {_flags, [child]}} =
                   ConsumerGroup.init(
                     {{KafkaEx.Consumer.GenConsumer, NoopConsumer}, @group, ["topic"],
                      [server_assignor: "range", rack_id: "rack-a", subscribed_topic_regex: "t.*", commit_interval: 10]}
                   )

          assert child.id == Manager
          # The classic path is unchanged: the options reach the Manager as before.
          {Manager, :start_link, [{_, _, _, opts}]} = child.start
          assert opts[:rack_id] == "rack-a"
        end)

      assert [[warning]] = Regex.scan(~r/Consumer group [^\n]*ignores the options [^\n]*/, log)
      assert warning =~ "[:server_assignor, :rack_id, :subscribed_topic_regex]"
      assert warning =~ "group_protocol: :classic"
    end

    test "no warning when every option applies" do
      log =
        capture_log(fn ->
          ConsumerGroup.init(
            {{KafkaEx.Consumer.GenConsumer, NoopConsumer}, @group, ["topic"],
             [heartbeat_interval: 1_000, commit_interval: 10, group_instance_id: "a"]}
          )

          ConsumerGroup.init(
            {{KafkaEx.Consumer.GenConsumer, NoopConsumer}, @group, ["topic"],
             [group_protocol: :consumer, server_assignor: "uniform", commit_interval: 10, group_instance_id: "a"]}
          )
        end)

      refute log =~ "ignores the options"
    end

    test "rack_id is optional under :consumer: no rack id is sent without it" do
      client = start_stub(with_heartbeat_api())

      capture_log(fn ->
        {:ok, _sup} = start_group(client: client, group_protocol: :consumer)
        assert_receive {:heartbeat_call, _from, @group, opts}, 2_000
        refute Keyword.has_key?(opts, :rack_id)
      end)
    end
  end

  describe "a cluster without the consumer group protocol" do
    test "fails the start with :consumer_group_protocol_not_supported, no fallback to classic" do
      # A supervisor that fails to start exits its (linked) caller.
      Process.flag(:trap_exit, true)
      client = start_stub(without_heartbeat_api())

      log =
        capture_log(fn ->
          assert {:error,
                  {:shutdown, {:failed_to_start_child, ConsumerProtocolManager, :consumer_group_protocol_not_supported}}} =
                   start_group(client: client, group_protocol: :consumer)
        end)

      assert log =~ "does not support the KIP-848 consumer group protocol"
      assert log =~ "group_protocol: :classic"
      # No heartbeat, and no classic JoinGroup either.
      refute_received {:heartbeat_call, _, _, _}
      assert Process.alive?(client)
    end

    test "an ApiVersions probe that fails decides nothing: the join's own error is terminal" do
      client = start_stub({:error, :timeout})

      capture_log(fn ->
        {:ok, sup} = start_group(client: client, group_protocol: :consumer)
        manager = ConsumerGroup.get_manager_pid(sup)
        ref = Process.monitor(manager)
        assert_receive {:heartbeat_call, from, @group, _opts}, 2_000
        GenServer.reply(from, {:error, :api_not_supported_by_broker})

        assert_receive {:DOWN, ^ref, :process, ^manager, {:shutdown, {:terminal, :api_not_supported_by_broker}}},
                       5_000
      end)
    end

    test "an ApiVersions probe that exits decides nothing: the group still starts" do
      {:ok, client} = DyingClient.start_link(nil)
      Process.unlink(client)
      ref = Process.monitor(client)

      capture_log(fn ->
        # Without the catch the exit escapes init and the start fails.
        assert {:ok, _sup} = start_group(client: client, group_protocol: :consumer)
        assert_receive {:DOWN, ^ref, :process, ^client, :client_crashed}, 2_000
      end)
    end

    test "a terminal :unsupported_version heartbeat names the protocol in its error log" do
      client = start_stub(with_heartbeat_api())

      log =
        capture_log(fn ->
          {:ok, sup} = start_group(client: client, group_protocol: :consumer)
          manager = ConsumerGroup.get_manager_pid(sup)
          ref = Process.monitor(manager)
          assert_receive {:heartbeat_call, from, @group, _opts}, 2_000
          GenServer.reply(from, {:error, :unsupported_version})
          assert_receive {:DOWN, ^ref, :process, ^manager, {:shutdown, {:terminal, :unsupported_version}}}, 5_000
        end)

      assert log =~ "may not support the KIP-848 consumer group protocol"
      assert log =~ "group_protocol: :classic"
    end
  end
end
