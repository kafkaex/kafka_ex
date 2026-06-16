# Fix #538: `start_client/1` crashes on missing config defaults Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `KafkaEx.API.start_client/1` and `KafkaEx.API.child_spec/1` merge `config.exs` defaults and accept the documented `:brokers` option, so `KafkaEx.API.start_client(brokers: [{"localhost", 9092}])` works instead of raising `InvalidConsumerGroupError: nil`.

**Architecture:** Centralize the "merge config defaults + normalize `:brokers` → `:uris`" logic in the existing public `KafkaEx.build_worker_options/1`. Route `start_client/1` and `child_spec/1` through it. `start_client/1` returns `{:error, :invalid_consumer_group}` on a bad group; `child_spec/1` raises `KafkaEx.InvalidConsumerGroupError` at spec-build time. `:brokers` becomes the canonical user-facing key; `:uris` keeps working as a silent (docs-only deprecated) alias. Internal `State.static_init/1` is untouched — it keeps reading `:uris`.

**Tech Stack:** Elixir, ExUnit, Mix. No new dependencies.

---

## Background / Root Cause

`KafkaEx.API.start_client/1` (`lib/kafka_ex/api.ex:265`) and `child_spec/1` (`lib/kafka_ex/api.ex:286`) pass options **raw** to `KafkaEx.Client.start_link/2`. They never call `KafkaEx.build_worker_options/1`, so:

1. **Config defaults are never merged.** `KafkaEx.Client.State.static_init/1` (`lib/kafka_ex/client/state.ex:50`) reads `consumer_group: Keyword.get(args, :consumer_group)` → `nil`. `KafkaEx.Client.init/1` (`lib/kafka_ex/client/client.ex:81-84`) then raises `KafkaEx.InvalidConsumerGroupError`. This is the crash in the issue.
2. **`:brokers` is the wrong key.** Docs/examples (README:100/120, UPGRADING:59/351, `api.ex:241`) tell users to pass `:brokers`, but `State.static_init/1` reads `:uris` (`lib/kafka_ex/client/state.ex:44`). `build_worker_options/1` only maps the **config** `:brokers` → `:uris`; it does not translate a user-passed `:brokers` keyword.

### Locked design decisions (from grilling)
1. `start_client/1` + `child_spec/1` auto-merge config defaults.
2. `:brokers` is canonical user key; `:uris` is a working alias. Translate `:brokers` → `:uris` at the boundary; an explicit `:uris` wins if both are given.
3. Centralize merge + normalize in `build_worker_options/1`.
4. `start_client/1` returns `{:error, :invalid_consumer_group}`; `child_spec/1` raises `KafkaEx.InvalidConsumerGroupError` eagerly.
5. Scope: core fix + fix all docs + document hidden options (`:initial_topics`, `:allow_auto_topic_creation`) + regression tests.
6. `:uris` deprecation is docs-only (no runtime warning).

### Key facts verified
- `build_worker_options/1` is **idempotent** — feeding its own output back in is a no-op merge, so the currently-documented `build_worker_options |> start_client` pattern stays safe.
- `KafkaEx.InvalidConsumerGroupError.exception/1` (`lib/kafka_ex/support/exceptions.ex:18-21`) takes the bad group value and formats `"Invalid consumer group: <inspect>"`.
- Test env (`config/config.exs`) sets `default_consumer_group: "kafka_ex"` and `brokers: [{"localhost",9092}, …]`, so `build_worker_options([])` succeeds in unit tests.
- `test/kafka_ex/api_test.exs:30` currently asserts `client_opts[:brokers] == [{"localhost", 9092}]` — this WILL break after normalization and must be updated.
- Integration tests are tagged (`:lifecycle` etc.) and **excluded by default** (`test/test_helper.exs`). They need a real broker.

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `lib/kafka_ex.ex` | `build_worker_options/1`: merge defaults, normalize `:brokers`→`:uris`; `worker_setting` type; docstrings | Modify |
| `lib/kafka_ex/api.ex` | `start_client/1` + `child_spec/1`: route through `build_worker_options/1`; docstrings | Modify |
| `test/kafka_ex_test.exs` | Unit tests for `build_worker_options/1` normalization + validation | Create |
| `test/kafka_ex/api_test.exs` | Unit tests for `child_spec/1` (update) + `start_client/1` error path (add) | Modify |
| `test/integration/lifecycle/start_client_test.exs` | Integration: `start_client(brokers:)` works end-to-end with config defaults | Modify |
| `README.md`, `UPGRADING.md` | Doc accuracy: canonical `:brokers`, config auto-merge, deprecate `:uris` | Modify |
| `CHANGELOG.md` | `## Unreleased` → `### Fixed` entry | Modify |

---

## Task 1: `build_worker_options/1` normalizes `:brokers` → `:uris`

**Files:**
- Modify: `lib/kafka_ex.ex:118-135` (the `build_worker_options/1` function)
- Test: `test/kafka_ex_test.exs` (create)

- [ ] **Step 1: Write the failing unit tests**

Create `test/kafka_ex_test.exs` with exactly this content:

```elixir
defmodule KafkaExTest do
  use ExUnit.Case, async: true

  describe "build_worker_options/1" do
    test "translates :brokers to :uris" do
      {:ok, opts} = KafkaEx.build_worker_options(brokers: [{"example", 9092}])

      assert opts[:uris] == [{"example", 9092}]
      refute Keyword.has_key?(opts, :brokers)
    end

    test "an explicit :uris wins when both :brokers and :uris are given" do
      {:ok, opts} = KafkaEx.build_worker_options(uris: [{"u", 1}], brokers: [{"b", 2}])

      assert opts[:uris] == [{"u", 1}]
      refute Keyword.has_key?(opts, :brokers)
    end

    test "user :brokers overrides the config-derived default uris" do
      {:ok, opts} = KafkaEx.build_worker_options(brokers: [{"custom", 1234}])

      assert opts[:uris] == [{"custom", 1234}]
    end

    test "merges config defaults when nothing is supplied" do
      {:ok, opts} = KafkaEx.build_worker_options([])

      # config/config.exs sets default_consumer_group: "kafka_ex"
      assert opts[:consumer_group] == "kafka_ex"
      assert is_list(opts[:uris])
    end

    test "returns error for an invalid (empty) consumer group" do
      assert {:error, :invalid_consumer_group} =
               KafkaEx.build_worker_options(consumer_group: "")
    end

    test "is idempotent — feeding its own output back in is a no-op" do
      {:ok, once} = KafkaEx.build_worker_options(brokers: [{"x", 1}])
      {:ok, twice} = KafkaEx.build_worker_options(once)

      assert Enum.sort(once) == Enum.sort(twice)
    end
  end
end
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `mix test test/kafka_ex_test.exs`
Expected: FAIL — the `:brokers`→`:uris` translation tests fail (e.g. `opts[:uris]` is the config default, and `Keyword.has_key?(opts, :brokers)` is `true`).

- [ ] **Step 3: Implement the normalization in `build_worker_options/1`**

In `lib/kafka_ex.ex`, replace the function body at lines 118-135:

```elixir
  def build_worker_options(worker_init) do
    defaults = [
      uris: Config.brokers(),
      consumer_group: Config.default_consumer_group(),
      use_ssl: Config.use_ssl(),
      ssl_options: Config.ssl_options(),
      auth: Config.auth_config()
    ]

    worker_init = Keyword.merge(defaults, worker_init)
    supplied_consumer_group = Keyword.get(worker_init, :consumer_group)

    if valid_consumer_group?(supplied_consumer_group) do
      {:ok, worker_init}
    else
      {:error, :invalid_consumer_group}
    end
  end
```

with:

```elixir
  def build_worker_options(worker_init) do
    worker_init = normalize_broker_key(worker_init)

    defaults = [
      uris: Config.brokers(),
      consumer_group: Config.default_consumer_group(),
      use_ssl: Config.use_ssl(),
      ssl_options: Config.ssl_options(),
      auth: Config.auth_config()
    ]

    worker_init = Keyword.merge(defaults, worker_init)
    supplied_consumer_group = Keyword.get(worker_init, :consumer_group)

    if valid_consumer_group?(supplied_consumer_group) do
      {:ok, worker_init}
    else
      {:error, :invalid_consumer_group}
    end
  end

  # `:brokers` is the canonical user-facing key (it matches the `:brokers`
  # config key). Internally everything reads `:uris`, so translate here.
  # We normalize BEFORE merging defaults so a user-supplied broker list
  # overrides the config-derived `:uris`. An explicit `:uris` wins if both
  # are given. Done here (not in static_init) so every entry point that
  # calls build_worker_options/1 — start_client, child_spec, create_worker,
  # start_link_worker — accepts `:brokers` uniformly.
  defp normalize_broker_key(worker_init) do
    case Keyword.pop(worker_init, :brokers) do
      {nil, rest} -> rest
      {brokers, rest} -> if Keyword.has_key?(rest, :uris), do: rest, else: Keyword.put(rest, :uris, brokers)
    end
  end
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `mix test test/kafka_ex_test.exs`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add lib/kafka_ex.ex test/kafka_ex_test.exs
git commit -m "fix(#538): build_worker_options/1 accepts :brokers as canonical key

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: `start_client/1` merges defaults and returns error tuple

**Files:**
- Modify: `lib/kafka_ex/api.ex:264-268` (`start_client/1`)
- Test: `test/kafka_ex/api_test.exs` (add a `describe "start_client/1"` block)

- [ ] **Step 1: Write the failing unit test (error path — no broker needed)**

In `test/kafka_ex/api_test.exs`, add this `describe` block immediately after the existing `describe "child_spec/1" do … end` block (after line 39):

```elixir
  describe "start_client/1" do
    test "returns {:error, :invalid_consumer_group} for an empty group (no crash)" do
      assert {:error, :invalid_consumer_group} =
               KafkaEx.API.start_client(brokers: [{"localhost", 9092}], consumer_group: "")
    end
  end
```

This works without a real broker because `build_worker_options/1` returns the error *before* any connection is attempted.

- [ ] **Step 2: Run the test to verify it fails**

Run: `mix test test/kafka_ex/api_test.exs:41` (use the line number where your new test lands)
Expected: FAIL — current `start_client/1` ignores `:consumer_group`, calls `Client.start_link`, and either crashes connecting or raises `InvalidConsumerGroupError` rather than returning `{:error, :invalid_consumer_group}`.

- [ ] **Step 3: Implement the merge + error-tuple return**

In `lib/kafka_ex/api.ex`, replace `start_client/1` at lines 264-268:

```elixir
  @spec start_client(opts) :: {:ok, pid} | {:error, term}
  def start_client(opts \\ []) do
    {name, client_opts} = Keyword.pop(opts, :name)
    Client.start_link(client_opts, name || :no_name)
  end
```

with:

```elixir
  @spec start_client(opts) :: {:ok, pid} | {:error, term}
  def start_client(opts \\ []) do
    {name, client_opts} = Keyword.pop(opts, :name)

    case KafkaEx.build_worker_options(client_opts) do
      {:ok, worker_opts} -> Client.start_link(worker_opts, name || :no_name)
      {:error, reason} -> {:error, reason}
    end
  end
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `mix test test/kafka_ex/api_test.exs`
Expected: PASS (existing `api_test.exs` tests still pass; the new `start_client/1` error-path test passes).

> Note: `child_spec/1` tests in this file will be addressed in Task 3 — if they fail now because `start_client` changes are unrelated, that's fine; Task 3 fixes them. (They should not fail from this task.)

- [ ] **Step 5: Commit**

```bash
git add lib/kafka_ex/api.ex test/kafka_ex/api_test.exs
git commit -m "fix(#538): start_client/1 merges config defaults, returns error tuple on bad group

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: `child_spec/1` merges defaults and raises eagerly on bad group

**Files:**
- Modify: `lib/kafka_ex/api.ex:285-295` (`child_spec/1`)
- Test: `test/kafka_ex/api_test.exs:20-39` (update existing assertions + add raise test)

- [ ] **Step 1: Update the existing `child_spec/1` tests and add a raise test**

In `test/kafka_ex/api_test.exs`, replace the entire existing block (lines 20-39):

```elixir
  describe "child_spec/1" do
    test "returns valid child spec" do
      spec = KafkaEx.API.child_spec(name: TestClient, brokers: [{"localhost", 9092}])

      assert spec.id == TestClient
      assert spec.type == :worker
      assert spec.restart == :permanent
      assert {KafkaEx.Client, :start_link, [client_opts, name]} = spec.start
      assert name == TestClient
      refute Keyword.has_key?(client_opts, :name)
      assert client_opts[:brokers] == [{"localhost", 9092}]
    end

    test "uses module name as default id" do
      spec = KafkaEx.API.child_spec(brokers: [{"localhost", 9092}])

      assert spec.id == KafkaEx.API
      assert {KafkaEx.Client, :start_link, [_client_opts, :no_name]} = spec.start
    end
  end
```

with:

```elixir
  describe "child_spec/1" do
    test "returns valid child spec with merged, normalized options" do
      spec = KafkaEx.API.child_spec(name: TestClient, brokers: [{"localhost", 9092}])

      assert spec.id == TestClient
      assert spec.type == :worker
      assert spec.restart == :permanent
      assert {KafkaEx.Client, :start_link, [client_opts, name]} = spec.start
      assert name == TestClient
      refute Keyword.has_key?(client_opts, :name)
      # :brokers is normalized to :uris, and config defaults are merged in
      assert client_opts[:uris] == [{"localhost", 9092}]
      refute Keyword.has_key?(client_opts, :brokers)
      assert client_opts[:consumer_group] == "kafka_ex"
    end

    test "uses module name as default id" do
      spec = KafkaEx.API.child_spec(brokers: [{"localhost", 9092}])

      assert spec.id == KafkaEx.API
      assert {KafkaEx.Client, :start_link, [_client_opts, :no_name]} = spec.start
    end

    test "raises InvalidConsumerGroupError at spec-build time for an empty group" do
      assert_raise KafkaEx.InvalidConsumerGroupError, fn ->
        KafkaEx.API.child_spec(brokers: [{"localhost", 9092}], consumer_group: "")
      end
    end
  end
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `mix test test/kafka_ex/api_test.exs`
Expected: FAIL — `client_opts[:uris]` assertion fails (current `child_spec` passes raw `:brokers`, never merges defaults), and the `assert_raise` test fails (current `child_spec` never validates).

- [ ] **Step 3: Implement the merge + eager raise**

In `lib/kafka_ex/api.ex`, replace `child_spec/1` at lines 285-295:

```elixir
  @spec child_spec(opts) :: Supervisor.child_spec()
  def child_spec(opts) do
    {name, client_opts} = Keyword.pop(opts, :name)

    %{
      id: name || __MODULE__,
      start: {KafkaEx.Client, :start_link, [client_opts, name || :no_name]},
      type: :worker,
      restart: :permanent
    }
  end
```

with:

```elixir
  @spec child_spec(opts) :: Supervisor.child_spec()
  def child_spec(opts) do
    {name, client_opts} = Keyword.pop(opts, :name)

    worker_opts =
      case KafkaEx.build_worker_options(client_opts) do
        {:ok, worker_opts} ->
          worker_opts

        {:error, :invalid_consumer_group} ->
          # child_spec/1 must return a spec map, so we cannot return an
          # error tuple. Raise eagerly at spec-build time (supervisor boot)
          # for a clear failure instead of a cryptic GenServer init crash.
          raise KafkaEx.InvalidConsumerGroupError, Keyword.get(client_opts, :consumer_group)
      end

    %{
      id: name || __MODULE__,
      start: {KafkaEx.Client, :start_link, [worker_opts, name || :no_name]},
      type: :worker,
      restart: :permanent
    }
  end
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `mix test test/kafka_ex/api_test.exs`
Expected: PASS (all `child_spec/1` and `start_client/1` unit tests green).

- [ ] **Step 5: Run the full unit suite to confirm nothing else regressed**

Run: `mix test`
Expected: PASS. (Integration tests stay excluded by default per `test/test_helper.exs`.)

- [ ] **Step 6: Commit**

```bash
git add lib/kafka_ex/api.ex test/kafka_ex/api_test.exs
git commit -m "fix(#538): child_spec/1 merges config defaults, raises on invalid group

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Integration test — `start_client(brokers:)` works end-to-end

**Files:**
- Modify: `test/integration/lifecycle/start_client_test.exs`

> This test requires a running Kafka broker on `localhost:9092` and is tagged `:lifecycle` (excluded by default). It proves the issue's exact reproduction now succeeds.

- [ ] **Step 1: Read the existing test setup**

Run: `sed -n '1,30p' test/integration/lifecycle/start_client_test.exs`
Expected: Confirm the module name, `@moduletag :lifecycle` (or per-test `@tag`), aliases (`alias KafkaEx.API`), and the `setup`/`build_worker_options` pattern used at line ~15.

- [ ] **Step 2: Add the regression test**

Add this test inside the existing test module (match the file's existing `@tag`/`@moduletag` style — if the module uses `@moduletag :lifecycle`, no per-test tag is needed; otherwise prefix with `@tag :lifecycle`):

```elixir
  @tag :lifecycle
  test "start_client/1 with :brokers and no build_worker_options inherits config defaults (issue #538)" do
    # Exact reproduction from issue #538: pass :brokers directly, rely on
    # config.exs default_consumer_group. Must NOT raise InvalidConsumerGroupError.
    assert {:ok, client} = KafkaEx.API.start_client(brokers: [{"localhost", 9092}])
    assert is_pid(client)

    on_exit(fn -> if Process.alive?(client), do: GenServer.stop(client) end)
  end

  @tag :lifecycle
  test "start_client/1 still accepts the legacy :uris alias" do
    assert {:ok, client} = KafkaEx.API.start_client(uris: [{"localhost", 9092}])
    assert is_pid(client)

    on_exit(fn -> if Process.alive?(client), do: GenServer.stop(client) end)
  end
```

> If `on_exit` is not already imported/used in the file, place the cleanup using whatever teardown pattern the existing tests use (check Step 1 output). The existing tests at lines 21-83 start clients and should show the established cleanup approach — mirror it.

- [ ] **Step 3: Run the integration test (requires a broker)**

Run: `mix test test/integration/lifecycle/start_client_test.exs --only lifecycle`
Expected: PASS if a broker is available on `localhost:9092`. If no broker is available locally, document that this test is broker-gated and verify it at least compiles: `mix test test/integration/lifecycle/start_client_test.exs --only lifecycle --exclude lifecycle` should report "0 failures" (all excluded) with no compile errors.

- [ ] **Step 4: Commit**

```bash
git add test/integration/lifecycle/start_client_test.exs
git commit -m "test(#538): integration coverage for start_client :brokers + :uris

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Documentation, types, and CHANGELOG

**Files:**
- Modify: `lib/kafka_ex/api.ex` (`start_client/1` docstring ~236-263, `child_spec/1` docstring ~270-284)
- Modify: `lib/kafka_ex.ex` (`worker_setting` type ~43-49; `create_worker/2` docstring ~51-62; `build_worker_options/1` docstring ~112-116)
- Modify: `README.md` (Configuration section ~132)
- Modify: `UPGRADING.md` (add option-key note)
- Modify: `CHANGELOG.md` (`## Unreleased` → `### Fixed`)

> No tests in this task — it's docs/typespecs only. Verify with `mix docs` (if available) or `mix compile --warnings-as-errors`.

- [ ] **Step 1: Update `start_client/1` docstring in `lib/kafka_ex/api.ex`**

Replace the `## Options` portion of the `start_client/1` `@doc` (lines 239-251) — currently:

```elixir
  ## Options

    * `:brokers` — list of `{host, port}` tuples. To inherit defaults from
      application config, use `KafkaEx.build_worker_options/1` first (see examples)
    * `:name` — process registration. Accepts any value valid for
      `GenServer.start_link/3`'s `name:` option:
        * an atom — local registration
        * `{:global, term}` — cluster-wide registration via `:global`
        * `{:via, Module, term}` — custom registry (e.g. `Registry`)
      If absent (or `nil`), the client is started unnamed and the pid
      is the identity.
    * Other options forwarded to `KafkaEx.Client.start_link/2`.
```

with:

```elixir
  Application config defaults (`config :kafka_ex, …`) are merged
  automatically — any option you pass overrides the corresponding default.

  ## Options

    * `:brokers` — list of `{host, port}` tuples. Defaults to the `:brokers`
      application config.
    * `:consumer_group` — consumer group for auto-commit, or
      `:no_consumer_group`. Defaults to the `:default_consumer_group` config.
    * `:use_ssl` — enable SSL (default from config, else `false`).
    * `:ssl_options` — Erlang `:ssl` options (default from config).
    * `:auth` — SASL auth config, see `KafkaEx.Auth.Config` (default from config).
    * `:metadata_update_interval` — metadata refresh interval in ms (default `30_000`).
    * `:allow_auto_topic_creation` — allow brokers to auto-create topics on
      metadata requests (default `true`).
    * `:initial_topics` — topics to fetch metadata for at startup (default `[]`).
    * `:uris` — **deprecated** alias for `:brokers`; still accepted.
    * `:name` — process registration. Accepts any value valid for
      `GenServer.start_link/3`'s `name:` option:
        * an atom — local registration
        * `{:global, term}` — cluster-wide registration via `:global`
        * `{:via, Module, term}` — custom registry (e.g. `Registry`)
      If absent (or `nil`), the client is started unnamed and the pid
      is the identity.

  Returns `{:error, :invalid_consumer_group}` if the resolved consumer
  group is invalid.
```

Then update the doctest examples (lines 252-262) — the `build_worker_options/1` step is no longer required. Replace:

```elixir
  ## Examples

      iex> {:ok, opts} = KafkaEx.build_worker_options([])
      iex> {:ok, client} = KafkaEx.API.start_client(opts)
      iex> is_pid(client)
      true

      iex> {:ok, opts} = KafkaEx.build_worker_options([])
      iex> {:ok, client} = KafkaEx.API.start_client([{:name, MyApp.KafkaClient} | opts])
      iex> client == Process.whereis(MyApp.KafkaClient)
      true
```

with:

```elixir
  ## Examples

      # Inherits brokers + consumer group from application config:
      {:ok, client} = KafkaEx.API.start_client()

      # Or pass brokers explicitly:
      {:ok, client} = KafkaEx.API.start_client(brokers: [{"localhost", 9092}])

      # With a registered name:
      {:ok, client} = KafkaEx.API.start_client(name: MyApp.KafkaClient, brokers: [{"localhost", 9092}])
```

> These are now plain code blocks (not `iex>` doctests) because they require a running broker and would fail under `mix test`'s doctest runner.

- [ ] **Step 2: Note the eager-raise behavior in `child_spec/1` docstring**

In `lib/kafka_ex/api.ex`, append to the `child_spec/1` `@doc` (after the existing example, before the closing `"""` near line 283):

```elixir
  Application config defaults are merged automatically (same as
  `start_client/1`). Because a child spec cannot carry an error tuple,
  an invalid consumer group raises `KafkaEx.InvalidConsumerGroupError`
  when the spec is built.
```

- [ ] **Step 3: Update `worker_setting` type and `create_worker/2` docstring in `lib/kafka_ex.ex`**

Replace the `worker_setting` type (lines 43-49):

```elixir
  @type worker_setting ::
          {:uris, uri}
          | {:consumer_group, binary | :no_consumer_group}
          | {:metadata_update_interval, non_neg_integer}
          | {:ssl_options, ssl_options}
          | {:auth, KafkaEx.Auth.Config.t() | nil}
          | {:initial_topics, [binary]}
```

with:

```elixir
  @type worker_setting ::
          {:brokers, uri}
          | {:uris, uri}
          | {:consumer_group, binary | :no_consumer_group}
          | {:metadata_update_interval, non_neg_integer}
          | {:use_ssl, boolean}
          | {:ssl_options, ssl_options}
          | {:auth, KafkaEx.Auth.Config.t() | nil}
          | {:allow_auto_topic_creation, boolean}
          | {:initial_topics, [binary]}
```

Then update the `create_worker/2` `## Options` list (lines 56-61) to make `:brokers` canonical and mark `:uris` deprecated:

```elixir
  - `consumer_group`: Name of the consumer group, `:no_consumer_group` for none
  - `uris`: List of brokers as `{"host", port}` tuples
  - `metadata_update_interval`: Metadata refresh interval in ms (default: 30000)
  - `use_ssl`: Enable SSL connections (default: false)
  - `ssl_options`: SSL options (see Erlang ssl docs)
  - `auth`: SASL authentication config (see `KafkaEx.Auth.Config`)
```

becomes:

```elixir
  - `brokers`: List of brokers as `{"host", port}` tuples
  - `consumer_group`: Name of the consumer group, `:no_consumer_group` for none
  - `metadata_update_interval`: Metadata refresh interval in ms (default: 30000)
  - `use_ssl`: Enable SSL connections (default: false)
  - `ssl_options`: SSL options (see Erlang ssl docs)
  - `auth`: SASL authentication config (see `KafkaEx.Auth.Config`)
  - `allow_auto_topic_creation`: Allow brokers to auto-create topics (default: true)
  - `initial_topics`: Topics to fetch metadata for at startup (default: [])
  - `uris`: **Deprecated** alias for `brokers`; still accepted
```

- [ ] **Step 4: Update `build_worker_options/1` docstring in `lib/kafka_ex.ex`**

Replace the `@doc` at lines 112-116:

```elixir
  @doc """
  Builds worker options by merging with application config defaults.

  Returns `{:error, :invalid_consumer_group}` if consumer group is invalid.
  """
```

with:

```elixir
  @doc """
  Builds worker options by merging with application config defaults.

  Normalizes the canonical `:brokers` option to the internal `:uris` key
  (an explicit `:uris` wins if both are given).

  Returns `{:error, :invalid_consumer_group}` if consumer group is invalid.
  """
```

- [ ] **Step 5: Clarify config auto-merge in `README.md`**

In `README.md`, replace line 132:

```markdown
KafkaEx can be configured via `config.exs` or by passing options directly to `KafkaEx.API.start_client/1`.
```

with:

```markdown
KafkaEx can be configured via `config.exs`, by passing options directly to
`KafkaEx.API.start_client/1`, or both — options passed to `start_client/1`
override the matching `config.exs` defaults. The broker option key is
`:brokers` (the legacy `:uris` is still accepted but deprecated).
```

> The existing examples at README lines 100/120 (`start_client(brokers: …)`) now work as written — leave them unchanged.

- [ ] **Step 6: Add an option-key note to `UPGRADING.md`**

In `UPGRADING.md`, immediately after the code block ending at line 60 (the `### API Changes` example), insert:

```markdown
> **Option key:** the broker list option is `:brokers` (matching the
> `config :kafka_ex, brokers:` key). The pre-1.0 `:uris` option is still
> accepted as a deprecated alias. `start_client/1` now merges `config.exs`
> defaults automatically, so you no longer need `KafkaEx.build_worker_options/1`
> just to inherit your configured brokers and consumer group.
```

- [ ] **Step 7: Add the CHANGELOG entry**

In `CHANGELOG.md`, under the existing `## Unreleased` → `### Fixed` heading (the `### Fixed` line is at the bottom of the shown header block), add as the first bullet:

```markdown
* **`KafkaEx.API.start_client/1` and `child_spec/1` now merge `config.exs`
  defaults and accept the documented `:brokers` option (#538).** Previously
  `start_client(brokers: [...])` crashed with `InvalidConsumerGroupError: nil`
  because config defaults were never merged and the `:brokers` key was ignored
  (the client read `:uris`). `:brokers` is now the canonical key; `:uris` is a
  deprecated-but-working alias. `start_client/1` returns
  `{:error, :invalid_consumer_group}` on a bad group; `child_spec/1` raises
  `KafkaEx.InvalidConsumerGroupError` at spec-build time.
```

- [ ] **Step 8: Verify docs compile cleanly**

Run: `mix compile --warnings-as-errors`
Expected: PASS, no warnings.

Run (if `ex_doc` is available): `mix docs`
Expected: builds without errors. If `mix docs` is not configured, skip.

- [ ] **Step 9: Run the full unit suite once more**

Run: `mix test`
Expected: PASS. Confirms the doctest changes (now plain code blocks) didn't leave a broken `iex>` doctest behind.

- [ ] **Step 10: Commit**

```bash
git add lib/kafka_ex.ex lib/kafka_ex/api.ex README.md UPGRADING.md CHANGELOG.md
git commit -m "docs(#538): document :brokers as canonical key, config auto-merge, deprecate :uris

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Self-Review

**1. Spec coverage:**
- Decision 1 (auto-merge defaults in `start_client`/`child_spec`) → Tasks 2, 3. ✅
- Decision 2 (`:brokers` canonical, `:uris` alias, translate at boundary, `:uris` wins if both) → Task 1 (+ tests for both-keys precedence). ✅
- Decision 3 (centralize in `build_worker_options/1`) → Task 1. ✅
- Decision 4 (`start_client` error tuple; `child_spec` raises) → Task 2 (error tuple), Task 3 (raise). ✅
- Decision 5 scope: core fix (Tasks 1-3), all docs (Task 5), hidden options `:initial_topics`/`:allow_auto_topic_creation` (Task 5 Steps 1, 3), regression tests (Tasks 1-4). ✅
- Decision 6 (`:uris` docs-only deprecation, no runtime warning) → Task 5 only; no code emits a warning. ✅

**2. Placeholder scan:** No TBD/TODO/"handle edge cases"/"similar to". All code shown in full. ✅

**3. Type consistency:**
- `KafkaEx.build_worker_options/1` returns `{:ok, opts} | {:error, :invalid_consumer_group}` — matched in Tasks 2 (`case … {:ok,…}/{:error,…}`) and 3 (`case … {:ok,…}/{:error, :invalid_consumer_group}`). ✅
- `normalize_broker_key/1` private helper defined in Task 1, referenced only inside `build_worker_options/1`. ✅
- `KafkaEx.InvalidConsumerGroupError.exception/1` takes the group value — used as `raise KafkaEx.InvalidConsumerGroupError, <value>` in Task 3 and asserted via `assert_raise KafkaEx.InvalidConsumerGroupError`. ✅
- Test assertion fix: `client_opts[:brokers]` → `client_opts[:uris]` (Task 3 Step 1) matches the normalization in Task 1. ✅

**Risk notes for the executor:**
- `test/kafka_ex/api_test.exs` is `async: true`; all new tests are read-only w.r.t. application env, so async is safe.
- Task 4 is broker-gated (`:lifecycle`); if no local broker, verify compilation only (Step 3 fallback).
- Line numbers drift as edits land — match on the quoted code, not the line number.
