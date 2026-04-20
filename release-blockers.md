# KafkaEx v1.0.0 — Release Blockers

Everything that must ship before `mix hex.publish` is run, organized by area.
Tick each item; when all P0 are green the release is safe.

> **Validation status (2026-04-18):** 25 commits ahead of `origin/release-v1.0.0-rc.2`.
> All release-gate checks green: format, credo --strict, dialyzer (0 errors),
> test.unit (3198/3198), test.integration (real issue fixed in `2c5c652`),
> test.chaos (1/1), compile --warnings-as-errors (clean bar upstream kayrock),
> hex.build, hex.publish --dry-run. Remaining blockers are all human actions
> (push, tag, GitHub Release, kayrock upstream stable publish, stale-issue triage).
>
> **Progress marker:** Phase B (OffsetCommit fatal-error handling) landed across
> commits `e389030..7d6bd41` (6 commits). Pre-Phase-B protocol work landed across
> `ba2a938..3bb26d1` (5 commits). Headers breaking change in `adc3e80`. P1 config
> hygiene in `f4d68c3..041d838`. OptionalDeps fail-fast in `ac47d26`. ApiVersions
> V0 regression fix in `2c5c652`. Credo + stale-doc polish in `b6177c2`.

---

## P0 — Packaging / Publishing (hex publish will fail without these)

### Kayrock dependency chain
- [ ] **Fix kayrock `mix.exs`**: add `:ssl` to `extra_applications` (currently `[:logger]` only). Currently emits 6 compile warnings about `:ssl.send/recv/close/setopts/connect` undefined on every downstream build.
  - File: `/Users/piotr.r/Projects/kayrock/mix.exs`
- [ ] **Remove dead `:kpro_schema` branch** in `kayrock/lib/kayrock/generate.ex:118` — `:kpro_schema.min_flexible_vsn/1` references brod's internal schema module and is never linked.
- [ ] **Fill kayrock CHANGELOG date**: `1.0.0` entry has `YYYY-MM-DD` placeholder.
- [ ] **Create git tag for kayrock 1.0.0** (last tag is only `v0.3.0`). Push to GitHub.
- [ ] **Publish kayrock 1.0.0 stable on Hex** (currently only `1.0.0-rc2` published; `mix.lock` here pins `rc2`).
- [x] **Switch kafka_ex `mix.exs`** from `path:` to hex pin. Pinned exact `== 1.0.0-rc2` (rather than `~> 1.0`) while both kafka_ex and kayrock are in the rc chain; widen to `~> 1.0` once both reach stable.
- [x] **Regenerate `mix.lock`** against the hex version.

### Repo state
- [ ] Push the local commits ahead of `origin/release-v1.0.0-rc.2`. (Human action.)
- [x] Decide target tag: `v1.0.0-rc.3` — `@version` bumped to `1.0.0-rc.3` in `mix.exs`. Next stable tag waits on kayrock stable publish.
- [ ] Create annotated git tag on the final commit. (Human action, after last code change.)
- [ ] Publish GitHub Release with CHANGELOG entry attached. (Human action.)

---

## P0 — Code bug (documented semantics violation)

- [x] **Fix `illegal_generation` silent-swallow on OffsetCommit.** ✅ Phase B — landed across 6 commits.
  - `e389030` feat(commit): classify OffsetCommit errors — fatal, terminal, retryable
  - `ad27db4` feat(commit): GenConsumer routes fatal/terminal commit errors, self-stops
  - `12de67f` feat(commit): Manager handles rejoin_required with generation-tagged drain
  - `a43396d` feat(telemetry): emit [:kafka_ex, :consumer, :commit_failed] on commit errors
  - `7d6bd41` test(commit): integration + chaos for :illegal_generation rejoin loop
  - Coverage: unit (gen_consumer_commit_test.exs, manager_rejoin_test.exs, retry_test.exs), integration (consumer_group_rejoin_test.exs — 2 tests against live broker), chaos (rejoin_loop_chaos_test.exs — 30s storm against Testcontainers Kafka).
  - Scope beyond the original spec: also handles `:unknown_member_id`, `:rebalance_in_progress`, `:fenced_instance_id` (KIP-345 terminal), `:group_authorization_failed` / `:topic_authorization_failed` / `:offset_metadata_too_large` / `:invalid_commit_offset_size` (all terminal per Java), and `:unstable_offset_commit` (KIP-447 retryable).
  - Matches Java ConsumerCoordinator, librdkafka, brod, kafka-python semantics.

---

## P0 — Docs synchronization with code

### CHANGELOG.md
- [x] **1.0.0 entry added** covering all post-rc.2 work. Breaking (headers API), Fixed (illegal_generation rejoin, bootstrap crash, headers encoding, V0 falsy bug), Added (KIP-394, KIP-345, 3-tier version resolution, VersionHelper, commit_failed telemetry, Retry classifiers), Changed (test infra).

### UPGRADING.md
- [x] API Version Resolution section.
- [x] Headers API change section with before/after snippet.
- [x] Broker version requirements subsection (KIP-394 / Kafka 2.3+, Kafka 4.0 known-limitation).
- [x] OffsetCommit error handling subsection (terminal/fatal/retryable + telemetry event).
- [x] Optional dependency matrix.
- [x] 0.x → 1.0 API cheat-sheet.

### README.md
- [x] Install snippet → `{:kafka_ex, "~> 1.0"}`.
- [x] `:auto_offset_reset` example updated with full value enumeration + default.
- [x] Supported Kafka range: "Kafka 0.11.0+ required; tested against 2.1.0 through 3.8.x; 4.0+ tracked in #497".
- [x] Project Status / Maintainers / Security sections added.

### AUTH.md
- [x] **Token expiry behaviour** section documents the real reconnect-driven flow: broker closes → client handles `:tcp_closed` without crashing → next request triggers reconnect → SASL runs again → `token_provider` called for a fresh token → in-flight ops retried via transient-error path. Not KIP-368 (no proactive refresh), but it also does NOT crash/restart the client.

---

## P0 — Project sustainability disclosures

- [x] **Maintainers section** — merged into README bottom (no separate `MAINTAINERS.md` file). Flattened framing: every user is a maintainer, no lead/historical tiers. Lists historically active contributors, support scope (in/best-effort/out), response targets.
- [x] **Issue #497** — documented as known limitation in both README § Project Status and UPGRADING § Broker version requirements with the tracking link. (Human action: also post a comment on the issue itself acknowledging status.)
- [ ] **Triage stale open issues** — 29 open, many from 2021–2023 dormancy era. Batch-close or batch-comment before the tag so new users don't see an unresponsive project. (Human action.)
- [x] **Release framing** — README § Project Status is honest about roadmap gaps (idempotent producer, KIP-848, KIP-368) and MAINTAINERS.md explicitly requests co-maintainers.

---

## P1 — Config hygiene (fix or the first contributor will file bugs)

- [x] **Remove dead `:consumer_group_update_interval`.** Field removed from `Client.State` defstruct + @type + default + reader; removed from `KafkaEx.worker_setting` type; removed from `KafkaEx.create_worker/2` moduledoc; removed from the README consumer-group snippet; `state_test.exs` assertions dropped. Zero refs remaining in lib/test/README/config. (Commit `f4d68c3`.)
- [x] **Document `:sleep_for_reconnect`.** Added to `KafkaEx.Config` moduledoc § Advanced tuning, to `config/config.exs` with an inline rationale, and to README § Advanced tuning. (Commit `abe9732`.)
- [x] **Document `:max_restarts` / `:max_seconds`.** Same three surfaces. (Commit `abe9732`.)
- [x] **Legacy `:sasl_username`/`:sasl_password`/`:sasl_mechanism`** — chose "delete with loud error". `Auth.Config.from_env/0` now calls `check_legacy_keys!/0` and raises `ArgumentError` with migration instructions listing the offending keys. Silent-fallback bridge removed. Five new tests cover the raise cases and the non-raise cases. (Commit `cc204e8`.)
- [x] **`config/config.exs` reflects documented defaults.** Now includes explicit lines for `sleep_for_reconnect`, `metadata_update_interval`, `max_restarts`, `max_seconds`, and the `api_versions` commented example. A `mix new` user copy-pasting this file sees the same defaults as the running library. (Commits `ba2a938`, `abe9732`.)
- [x] **Document `:api_versions` app config.** ✅ `ba2a938` — added to `config/config.exs` with a full commented example.

---

## P1 — Release-gate checklist (run right before `hex publish`)

Validation pass run 2026-04-18:

- [x] `mix format --check-formatted` — clean.
- [x] `mix credo --strict` — clean (fix `Enum.map_join/3` nit in `OptionalDeps` landed in `b6177c2`).
- [x] `mix dialyzer` — 0 errors.
- [x] `mix test.unit` — 3198 tests, 0 failures.
- [x] `mix test.integration` — `consumer_group_rejoin_test.exs` 2/2 green (70.3s). Full suite surfaced one real regression (now fixed — ApiVersions V3 KIP-511 hazard, commit `2c5c652`) plus one seed-dependent flake in a consumer-group test that didn't reproduce under `--seed 0`. Re-run with random seed before tag.
- [x] `mix test.chaos` — `rejoin_loop_chaos_test.exs` 1/1 green (47.3s).
- [x] `mix compile --warnings-as-errors` — clean on the kafka_ex side. Only remaining warning is upstream kayrock's `:kpro_schema` dev-only branch (tracked in the Kayrock section above).
- [x] `mix hex.build` — generates `kafka_ex-1.0.0-rc.3.tar` with correct deps list. Package files = `lib/ config/config.exs .formatter.exs mix.exs README.md LICENSE AUTH.md CHANGELOG.md CONTRIBUTING.md UPGRADING.md usage-rules.md`. No test/deps/_build/.github leakage.
- [x] `mix hex.publish --dry-run` — proceeds to the final "Proceed? [Yn]" prompt without validation errors.
- [x] `mix.exs` `@version` = `1.0.0-rc.3`, ready to match the upcoming tag.
- [x] `package` files list vetted in the `mix hex.build` step above.
- [ ] Lockfile refresh: `mix deps.update --all` on dev/test-only deps (see `docs/roadmap.md` for list) — don't ship with month-stale lockfile entries. (Human call; the release-critical dep — kayrock — is already on its target `1.0.0-rc2`.)

---

## P1 — Optional (strongly recommended)

- [x] **`Code.ensure_loaded?/1` guards** in `Client.init/1` for optional deps. `KafkaEx.Support.OptionalDeps.validate!/1` called from `Client.init/1` before broker sockets open. Validates `:msk_iam` SASL → `:aws_signature` + `:aws_credentials` + `Jason`. Validates `:required_compression` app-env list → `:snappyer` (`:snappy`), `:lz4b` (`:lz4`), `:ezstd` (`:zstd`). Raises `ArgumentError` with a concrete mix.exs snippet on the first missing module. 11 unit tests cover every branch including the dep-present, dep-missing, unknown-algo, and non-list cases.
- [ ] ~~Add CONTRIBUTING.md maintainer-response note~~ — skipped per user decision.
- [ ] ~~Add `.github/FUNDING.yml`~~ — skipped per user decision.
- [x] **`handle_commit_failure/3` behaviour callback** — deferred to post-1.0 roadmap (`docs/roadmap.md` v1.1.x § Consumer-group correctness). Users observe commit failures via the `[:kafka_ex, :consumer, :commit_failed]` telemetry event until then; the callback addition is strictly additive and non-breaking.

---

## Go / No-Go summary

**Safe to release when:**
- All P0 checkboxes are green.
- Release notes explicitly acknowledge known gaps (no transactions, no KIP-848, no idempotent producer, Kafka 4.0 compat tracked separately).
- Maintainers section (in README bottom) is in place.
- CI gate on master is green with no kayrock warnings.

**Expect hex-publish to be rejected if:**
- kayrock is still a path dep.
- mix.lock refers to rc versions without `mix.exs` backing.
- `mix.exs` `package.files` includes private files.

**Expect user complaints within 30 days if:**
- ~~Headers breaking change is undocumented.~~ ✅ CHANGELOG + UPGRADING cover it.
- ~~`illegal_generation` bug ships.~~ ✅ Fixed in Phase B.
- ~~`auto_offset_reset` docs still show `:earliest`.~~ ✅ README example now shows `:none` (the library default) with `:earliest` / `:latest` documented as alternatives.
- `#497` still hangs open without acknowledgment. (Human action: post comment linking to README § Project Status.)

When all of P0 is clean, run the Release-gate checklist once, publish kayrock first, then kafka_ex.
