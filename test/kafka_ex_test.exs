defmodule KafkaExTest do
  use ExUnit.Case, async: false

  test "Setting disable_default_worker to true removes the KafkaEx.Server worker" do
    # stop the application, disable the default worker, restart the application
    :ok = Application.stop(:kafka_ex)
    Application.put_env(:kafka_ex, :disable_default_worker, true)
    {:ok, _} = Application.ensure_all_started(:kafka_ex)

    # the supervisor should now have no children and the default worker should not be registered
    assert [] == Supervisor.which_children(KafkaEx.Supervisor)
    assert nil == Process.whereis(KafkaEx.Server)

    # revert the change, restart the application
    :ok = Application.stop(:kafka_ex)
    Application.put_env(:kafka_ex, :disable_default_worker, false)
    {:ok, _} = Application.ensure_all_started(:kafka_ex)

    # we should have the default worker back again
    pid = Process.whereis(KafkaEx.Server)
    assert is_pid(pid)
    assert [{:undefined, pid, :worker, [KafkaEx.server]}] ==
      Supervisor.which_children(KafkaEx.Supervisor)
  end

  test "worker spec" do
    name = :pr
    brokers = Application.get_env(:kafka_ex, :brokers)
    group = Application.get_env(:kafka_ex, :consumer_group)
    args = [[uris: brokers, consumer_group: group], name]
    assert(
      {:ok, {mod, {mod, :start_link, ^args}, :permanent, 5000, :worker, [mod]}}
      = KafkaEx.worker_spec(name)
    )
  end

  test "worker spec with options" do
    name = :pr
    {brokers, group, timeout} = {[{"localhost", 9092}], "foo", 2000}
    opts = [uris: brokers, consumer_group: group, sync_timeout: timeout]
    args = [opts, name]
    assert(
      {:ok, {mod, {mod, :start_link, ^args}, :permanent, 5000, :worker, [mod]}}
      = KafkaEx.worker_spec(name, opts)
    )
  end

  test "worker spec with invalid options" do
    assert {:error, :invalid_consumer_group}
      = KafkaEx.worker_spec(:pr, consumer_group: nil)
  end

end
