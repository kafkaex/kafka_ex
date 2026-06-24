defmodule KafkaEx.Integration.Lifecycle.StartClientTest do
  @moduledoc """
  Integration tests for KafkaEx.API.start_client/1 and child_spec/1 honoring
  the :name option (issue #416). All tests need a running broker.
  """
  use ExUnit.Case, async: true
  @moduletag :lifecycle

  alias KafkaEx.API
  alias KafkaEx.Client

  import KafkaEx.TestSupport.ProcessHelpers

  doctest KafkaEx.API

  setup do
    {:ok, args} = KafkaEx.build_worker_options([])
    {:ok, broker_opts: args}
  end

  describe "start_client/1 default (no :name)" do
    test "returns a pid and does NOT register globally", %{broker_opts: opts} do
      {:ok, pid} = API.start_client(opts)
      on_exit(fn -> stop_safely(pid) end)

      assert is_pid(pid)
      assert Process.whereis(KafkaEx.Client) == nil
    end

    test "name: nil is equivalent to omitting :name", %{broker_opts: opts} do
      {:ok, pid} = API.start_client(Keyword.put(opts, :name, nil))
      on_exit(fn -> stop_safely(pid) end)

      assert is_pid(pid)
      assert Process.whereis(KafkaEx.Client) == nil
    end
  end

  describe "start_client/1 with atom name" do
    test "registers the pid under the given atom", %{broker_opts: opts} do
      name = :"client_#{System.unique_integer([:positive])}"
      {:ok, pid} = API.start_client(Keyword.put(opts, :name, name))
      on_exit(fn -> stop_safely(pid) end)

      assert Process.whereis(name) == pid
    end

    test "two clients with distinct atom names coexist", %{broker_opts: opts} do
      name_a = :"client_a_#{System.unique_integer([:positive])}"
      name_b = :"client_b_#{System.unique_integer([:positive])}"

      {:ok, pid_a} = API.start_client(Keyword.put(opts, :name, name_a))
      {:ok, pid_b} = API.start_client(Keyword.put(opts, :name, name_b))

      on_exit(fn ->
        stop_safely(pid_a)
        stop_safely(pid_b)
      end)

      assert pid_a != pid_b
      assert Process.whereis(name_a) == pid_a
      assert Process.whereis(name_b) == pid_b
    end
  end

  describe "start_client/1 with :global name" do
    test "registers the pid globally", %{broker_opts: opts} do
      name = :"global_client_#{System.unique_integer([:positive])}"
      {:ok, pid} = API.start_client(Keyword.put(opts, :name, {:global, name}))
      on_exit(fn -> stop_safely({:global, name}) end)

      assert :global.whereis_name(name) == pid
    end
  end

  describe "start_client/1 with :via registry" do
    setup do
      registry = :"reg_#{System.unique_integer([:positive])}"
      start_supervised!({Registry, keys: :unique, name: registry})
      {:ok, registry: registry}
    end

    test "registers the pid via Registry", %{broker_opts: opts, registry: registry} do
      via_name = {:via, Registry, {registry, "client-1"}}
      {:ok, pid} = API.start_client(Keyword.put(opts, :name, via_name))
      on_exit(fn -> stop_safely(via_name) end)

      assert [{^pid, _value}] = Registry.lookup(registry, "client-1")
    end
  end

  describe "child_spec/1" do
    test "produces a spec with :id equal to :name", %{broker_opts: opts} do
      name = :"spec_client_#{System.unique_integer([:positive])}"
      spec_opts = Keyword.put(opts, :name, name)

      spec = API.child_spec(spec_opts)

      assert spec.id == name
      assert spec.start == {Client, :start_link, [Keyword.delete(spec_opts, :name), name]}
      assert spec.type == :worker
      assert spec.restart == :permanent
    end

    test "supervised children with distinct :name run independently", %{broker_opts: opts} do
      name_a = :"sup_a_#{System.unique_integer([:positive])}"
      name_b = :"sup_b_#{System.unique_integer([:positive])}"

      pid_a = start_supervised!(Supervisor.child_spec({API, Keyword.put(opts, :name, name_a)}, id: :a))
      pid_b = start_supervised!(Supervisor.child_spec({API, Keyword.put(opts, :name, name_b)}, id: :b))

      assert Process.alive?(pid_a)
      assert Process.alive?(pid_b)
      assert Process.whereis(name_a) == pid_a
      assert Process.whereis(name_b) == pid_b

      # Kill A; ExUnit's supervisor restarts it with a new pid under the same registered name.
      Process.exit(pid_a, :kill)

      restarted_a =
        wait_until(fn -> Process.whereis(name_a) end, fn p -> is_pid(p) and p != pid_a end, 10_000)

      assert is_pid(restarted_a)
      assert restarted_a != pid_a
      # B is untouched.
      assert Process.whereis(name_b) == pid_b

      # Restarted client is functional: its Kafka bootstrap completed and it
      # accepts API calls under its registered name.
      assert {:ok, _metadata} = API.metadata(name_a)
    end
  end

  describe "start_client/1 config-default merging (issue #538)" do
    test "with :brokers and no build_worker_options inherits config defaults" do
      # Exact reproduction from issue #538: pass :brokers directly, rely on
      # config.exs default_consumer_group. Must NOT raise InvalidConsumerGroupError.
      assert {:ok, client} = API.start_client(brokers: [{"localhost", 9092}])
      on_exit(fn -> stop_safely(client) end)

      assert is_pid(client)
    end

    test "still accepts the legacy :uris alias" do
      assert {:ok, client} = API.start_client(uris: [{"localhost", 9092}])
      on_exit(fn -> stop_safely(client) end)

      assert is_pid(client)
    end
  end

  defp wait_until(getter, predicate, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_until(getter, predicate, deadline)
  end

  defp do_wait_until(getter, predicate, deadline) do
    value = getter.()

    cond do
      predicate.(value) ->
        value

      System.monotonic_time(:millisecond) >= deadline ->
        value

      true ->
        Process.sleep(20)
        do_wait_until(getter, predicate, deadline)
    end
  end
end
