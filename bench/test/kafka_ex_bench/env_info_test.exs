defmodule KafkaExBench.EnvInfoTest do
  use ExUnit.Case, async: true
  alias KafkaExBench.EnvInfo

  test "direct profile records runtime and build facts without latency fields" do
    env = EnvInfo.collect(:direct, [])

    assert env.profile == :direct
    assert env.latency_ms == nil
    assert env.toxiproxy_version == nil
    assert env.otp == to_string(:erlang.system_info(:otp_release))
    assert env.elixir == System.version()
    assert env.schedulers_online == :erlang.system_info(:schedulers_online)
    assert env.deps |> Map.keys() |> Enum.sort() == [:benchee, :jason, :kayrock, :telemetry]
    assert Enum.all?(Map.values(env.deps), &(is_binary(&1) or is_nil(&1)))
    assert is_binary(env.kafka_ex_vsn)
    assert is_boolean(env.kafka_ex_dirty)
    assert env.kafka_ex_sha =~ ~r/\A[0-9a-f]{40}\z/
  end

  test "harness_sha is the commit of the repository containing bench" do
    assert EnvInfo.collect(:direct, []).harness_sha =~ ~r/\A[0-9a-f]{40}\z/
  end

  test "collect records whether the harness tree is dirty" do
    assert is_boolean(EnvInfo.collect(:direct, []).harness_dirty)
  end

  test "latency profile takes overrides" do
    env = EnvInfo.collect(:latency, latency_ms: 7, toxiproxy_version: "2.9.0")
    assert env.latency_ms == 7
    assert env.toxiproxy_version == "2.9.0"
  end

  test "encoded env contains no hostname, home or cwd" do
    json = :direct |> EnvInfo.collect([]) |> Jason.encode!()

    for private <- [to_string(:net_adm.localhost()), System.user_home!(), File.cwd!()] do
      refute String.contains?(json, private)
    end
  end

  test "app_version is nil for an app that is not loaded" do
    assert EnvInfo.app_version(:no_such_app_anywhere) == nil
    assert is_binary(EnvInfo.app_version(:jason))
  end

  describe "harness_dirty/1" do
    setup do
      repo = Path.join(Mix.Project.build_path(), "harness_dirty_#{System.unique_integer([:positive])}")
      bench = Path.join(repo, "bench")
      File.mkdir_p!(Path.join(bench, "lib"))
      File.mkdir_p!(Path.join(repo, "other"))
      File.write!(Path.join([bench, "lib", "a.ex"]), "x")
      File.write!(Path.join([repo, "other", "b.ex"]), "x")
      git!(repo, ["init", "-q"])
      git!(repo, ["add", "."])
      git!(repo, ["-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "init"])
      on_exit(fn -> File.rm_rf!(repo) end)
      {:ok, repo: repo, bench: bench}
    end

    test "a clean harness is not dirty", %{bench: bench} do
      assert EnvInfo.harness_dirty(bench) == false
    end

    test "a modified tracked file under bench counts", %{bench: bench} do
      File.write!(Path.join([bench, "lib", "a.ex"]), "y")
      assert EnvInfo.harness_dirty(bench) == true
    end

    test "an untracked source file under bench counts", %{bench: bench} do
      File.write!(Path.join([bench, "lib", "new.ex"]), "y")
      assert EnvInfo.harness_dirty(bench) == true
    end

    test "build output, deps and untracked tmp results are ignored", %{bench: bench} do
      for dir <- ["_build_candidate/dev", "_build/test", "deps/jason", "results/tmp/run"] do
        File.mkdir_p!(Path.join(bench, dir))
        File.write!(Path.join([bench, dir, "f"]), "y")
      end

      assert EnvInfo.harness_dirty(bench) == false
    end

    test "changes outside bench do not count", %{repo: repo, bench: bench} do
      File.write!(Path.join([repo, "other", "b.ex"]), "y")
      File.write!(Path.join([repo, "other", "new.ex"]), "y")
      assert EnvInfo.harness_dirty(bench) == false
    end

    test "a directory outside any repository is unknown" do
      dir = Path.join(System.tmp_dir!(), "no_repo_#{System.unique_integer([:positive])}")
      File.mkdir_p!(dir)
      on_exit(fn -> File.rm_rf!(dir) end)
      assert EnvInfo.harness_dirty(dir) == nil
    end
  end

  describe "git_state/1 dirtiness" do
    setup do
      dir = Path.join(Mix.Project.build_path(), "git_state_#{System.unique_integer([:positive])}")
      File.mkdir_p!(Path.join(dir, "lib"))
      File.mkdir_p!(Path.join(dir, "docs"))
      File.write!(Path.join(dir, "mix.exs"), "x")
      git!(dir, ["init", "-q"])
      git!(dir, ["add", "."])
      git!(dir, ["-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "init", "--allow-empty"])
      on_exit(fn -> File.rm_rf!(dir) end)
      {:ok, dir: dir}
    end

    test "a clean tree is not dirty", %{dir: dir} do
      assert {sha, false} = EnvInfo.git_state(dir)
      assert sha =~ ~r/\A[0-9a-f]{40}\z/
    end

    test "an untracked docs file does not count", %{dir: dir} do
      File.write!(Path.join([dir, "docs", "note.md"]), "x")
      assert {_, false} = EnvInfo.git_state(dir)
    end

    test "an untracked lib file counts", %{dir: dir} do
      File.write!(Path.join([dir, "lib", "new.ex"]), "x")
      assert {_, true} = EnvInfo.git_state(dir)
    end

    test "a modified mix.exs counts", %{dir: dir} do
      File.write!(Path.join(dir, "mix.exs"), "y")
      assert {_, true} = EnvInfo.git_state(dir)
    end

    defp git!(dir, args), do: {_, 0} = System.cmd("git", ["-C", dir | args], stderr_to_stdout: true)
  end
end
