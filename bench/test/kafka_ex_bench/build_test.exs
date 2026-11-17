defmodule KafkaExBench.BuildTest do
  use ExUnit.Case, async: true
  alias KafkaExBench.Build

  test "source_under? accepts files below the root" do
    assert Build.source_under?("/work/kafka_ex/lib/kafka_ex/api.ex", "/work/kafka_ex")
  end

  test "source_under? rejects sibling directories sharing the prefix" do
    refute Build.source_under?("/work/kafka_ex-other/lib/kafka_ex/api.ex", "/work/kafka_ex")
    refute Build.source_under?("/work/kafka_ex", "/work/kafka_ex")
  end

  test "kafka_ex_path defaults to the parent of the bench project" do
    assert Build.kafka_ex_path() == Path.expand(System.get_env("KAFKA_EX_PATH", ".."), File.cwd!())
  end

  describe "canonical" do
    setup do
      base = Path.join(System.tmp_dir!(), "bench-canonical-#{System.unique_integer([:positive])}")
      real = Path.join(base, "real/nested")
      File.mkdir_p!(real)
      File.ln_s!(Path.join(base, "real"), Path.join(base, "link"))
      on_exit(fn -> File.rm_rf!(base) end)
      {:ok, real: real, link: Path.join(base, "link")}
    end

    test "resolves a symlinked component", %{real: real, link: link} do
      assert Build.canonical(Path.join(link, "nested")) == Build.canonical(real)
    end

    test "keeps components that do not exist yet", %{link: link} do
      assert Build.canonical(Path.join(link, "missing/file.ex")) == Path.join(Build.canonical(link), "missing/file.ex")
    end

    test "source_under? sees through a symlinked root", %{real: real, link: link} do
      assert Build.source_under?(Path.join(real, "api.ex"), link)
      assert Build.source_under?(Path.join(link, "nested/api.ex"), real)
    end
  end
end
