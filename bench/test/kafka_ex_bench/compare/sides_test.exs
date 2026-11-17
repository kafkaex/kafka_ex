defmodule KafkaExBench.Compare.SidesTest do
  use ExUnit.Case, async: true
  alias KafkaExBench.Compare.Sides

  @moduletag :tmp_dir

  defp git!(dir, args) do
    {out, 0} = System.cmd("git", ["-C", dir | args], stderr_to_stdout: true)
    String.trim(out)
  end

  defp repo!(dir) do
    root = Path.join(dir, "repo")
    File.mkdir_p!(Path.join(root, "lib"))
    File.write!(Path.join(root, "lib/a.ex"), "a")
    File.write!(Path.join(root, "mix.exs"), "m")
    git!(root, ["init", "-q"])
    git!(root, ["-c", "user.name=t", "-c", "user.email=t@t", "add", "."])
    git!(root, ["-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "one"])
    root
  end

  test "a path side is used as is and records its sha", %{tmp_dir: dir} do
    root = repo!(dir)
    sha = git!(root, ["rev-parse", "HEAD"])

    assert {:ok, side, nil} =
             Sides.prepare(root, "base",
               repo_root: root,
               worktree_root: Path.join(dir, "wt"),
               allow_dirty: false,
               env: []
             )

    assert %{label: "base", path: ^root, sha: ^sha, env: []} = side
  end

  test "a dirty path side is refused unless allowed", %{tmp_dir: dir} do
    root = repo!(dir)
    File.write!(Path.join(root, "lib/a.ex"), "changed")
    opts = [repo_root: root, worktree_root: Path.join(dir, "wt"), env: []]
    assert {:error, message} = Sides.prepare(root, "candidate", [allow_dirty: false] ++ opts)
    assert message =~ "uncommitted changes"
    assert {:ok, _side, nil} = Sides.prepare(root, "candidate", [allow_dirty: true] ++ opts)
  end

  test "a sha side becomes a detached worktree that with_sides removes", %{tmp_dir: dir} do
    root = repo!(dir)
    sha = git!(root, ["rev-parse", "HEAD"])
    config = %{base: sha, candidate: root, allow_dirty: false, candidate_env: [{"K", "V"}]}
    wt_root = Path.join(dir, "wt")

    seen =
      Sides.with_sides(config, [repo_root: root, worktree_root: wt_root], fn sides ->
        assert File.exists?(Path.join(sides.base.path, "lib/a.ex"))
        assert sides.base.sha == sha
        assert sides.candidate.env == [{"K", "V"}]
        assert sides.base.env == []
        sides.base.path
      end)

    refute File.exists?(seen)
    refute File.exists?(wt_root)
    assert git!(root, ["worktree", "list"]) =~ "repo"
    refute git!(root, ["worktree", "list"]) =~ "wt/base"
  end

  test "worktrees are removed when the body raises", %{tmp_dir: dir} do
    root = repo!(dir)
    sha = git!(root, ["rev-parse", "HEAD"])
    config = %{base: sha, candidate: sha, allow_dirty: false, candidate_env: []}
    wt_root = Path.join(dir, "wt")

    assert_raise RuntimeError, "boom", fn ->
      Sides.with_sides(config, [repo_root: root, worktree_root: wt_root], fn _ -> raise "boom" end)
    end

    refute File.exists?(Path.join(wt_root, "base"))
    refute File.exists?(Path.join(wt_root, "candidate"))
  end

  test "an unknown revision is refused", %{tmp_dir: dir} do
    root = repo!(dir)

    assert {:error, message} =
             Sides.prepare("nope-ref", "base",
               repo_root: root,
               worktree_root: Path.join(dir, "wt"),
               allow_dirty: false,
               env: []
             )

    assert message =~ "nope-ref"
  end
end
