# KafkaEx v1.0.0 — Release Blockers

Everything that must ship before `mix hex.publish` is run, organized by area.
Tick each item; when all P0 are green the release is safe.

> **Progress marker (2026-04-18):** Phase B (OffsetCommit fatal-error handling)
> landed across commits `e389030..7d6bd41` (6 commits). Pre-Phase-B protocol
> work (3-tier API version resolution, KIP-394 JoinGroup, KIP-345 LeaveGroup,
> bootstrap crash fix, lifecycle test harness) landed across
> `ba2a938..3bb26d1` (5 commits). The produce-headers breaking change landed
> in `adc3e80`. Remaining work is packaging (kayrock hex-publish, `mix.exs`
> revert to hex pin, git tag, GitHub release) plus documentation catch-up in
> CHANGELOG.md / README.md / UPGRADING.md / MAINTAINERS.md.

---

## P0 — Packaging / Publishing (hex publish will fail without these)

### Kayrock dependency chain
- [ ] **Fix kayrock `mix.exs`**: add `:ssl` to `extra_applications` (currently `[:logger]` only). Currently emits 6 compile warnings about `:ssl.send/recv/close/setopts/connect` undefined on every downstream build.
  - File: `/Users/piotr.r/Projects/kayrock/mix.exs`
- [ ] **Remove dead `:kpro_schema` branch** in `kayrock/lib/kayrock/generate.ex:118` — `:kpro_schema.min_flexible_vsn/1` references brod's internal schema module and is never linked.
- [ ] **Fill kayrock CHANGELOG date**: `1.0.0` entry has `YYYY-MM-DD` placeholder.
- [ ] **Create git tag for kayrock 1.0.0** (last tag is only `v0.3.0`). Push to GitHub.
- [ ] **Publish kayrock 1.0.0 stable on Hex** (currently only `1.0.0-rc2` published; `mix.lock` here pins `rc2`).
- [ ] **Switch kafka_ex `mix.exs`** from `{:kayrock, path: "../kayrock"}` back to `{:kayrock, "~> 1.0"}` (currently reverted to path for Phase B iteration — see commit `25be4f4`).
- [ ] **Regenerate `mix.lock`** against the published hex version.

### Repo state
- [ ] Push the 18 local commits ahead of `origin/release-v1.0.0-rc.2`.
- [ ] Decide target tag: `v1.0.0-rc.3` (safer, iterate) or `v1.0.0` (final).
- [ ] Restore `@version` to the target tag in `mix.exs` (currently `1.0.0` — the rc.3 bump was reverted during Phase B iteration).
- [ ] Create annotated git tag on the final commit.
- [ ] Publish GitHub Release with CHANGELOG entry attached.

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

### CHANGELOG.md — missing `1.0.0` entry
Post-rc.2 commits + the uncommitted headers change are unmentioned. The entry must cover:

- [ ] **Breaking — headers API:** `produce` / `produce_one` / `produce_sync` `headers:` option changed from `[{binary, binary}]` tuples to `[%KafkaEx.Messages.Header{}]` structs. Show before/after code snippet. (Commit `adc3e80`.)
- [x] **Fix — `illegal_generation` no longer swallowed.** ✅ Phase B. Include the expanded error-code coverage (terminal errors, :rebalance_in_progress reclassified retryable) and the `[:kafka_ex, :consumer, :commit_failed]` telemetry event in the entry.
- [ ] **Feat — KIP-394 two-step JoinGroup** (`a5794a7`): client auto-retries on `:member_id_required` with broker-assigned member_id. Required for Kafka 2.3+ under `group.initial.rebalance.delay.ms` semantics.
- [ ] **Feat — KIP-345 batch LeaveGroup V3+** (`ba2a938`): `LeaveGroup` sends `members` array for V3+ brokers.
- [ ] **Feat — 3-tier API version resolution** (`ba2a938`, `a5794a7`): per-request opt → app `api_versions` config → broker-negotiated max. Previously used hardcoded defaults regardless of broker capability. Includes V0 falsy-bug fix.
- [ ] **Fix — bootstrap crash** (`a5794a7`): client honours explicit `api_version` when broker map is empty.
- [ ] **Fix — headers encoding** (`603d10f` in history): per-record headers properly serialized in V3+ record batches.
- [ ] **Test — lifecycle integration tests for api-version resolution** (`3bb26d1`, `f3f1763`).

### UPGRADING.md
- [x] **API Version Resolution section** added (`ba2a938`). Documents the 3-tier resolution order.
- [ ] Add section **"Headers API change"** with before/after snippet and migration example.
- [ ] Add subsection **"Broker version requirements"**: call out KIP-394 (Kafka 2.3+) and KIP-345 static membership impact.
- [ ] Add subsection **"OffsetCommit error handling"**: document the new terminal / fatal-rejoin / retryable classes, the `[:kafka_ex, :consumer, :commit_failed]` telemetry event, and at-least-once redelivery after a rejoin. See GenConsumer moduledoc for the full table.
- [ ] Add **"Optional dependency matrix"** table — users silently hit `UndefinedFunctionError` if they use features without adding the deps:
  | Feature | Required dep |
  |---|---|
  | `:snappy` compression | `{:snappyer, "~> 1.2"}` |
  | `:msk_iam` SASL | `{:jason, "~> 1.0"}`, `{:aws_signature, "~> 0.4"}`, `{:aws_credentials, "~> 1.0"}` |
  | OAuth JWT parsing (user-side) | user's choice |
- [ ] Add **"0.x → 1.0 API cheat-sheet"** table:
  | 0.x | 1.0 |
  |---|---|
  | `KafkaEx.produce("t", 0, "m")` | `KafkaEx.API.produce_one(client, "t", 0, "m")` |
  | `KafkaEx.fetch("t", 0, offset: 0)` | `KafkaEx.API.fetch(client, "t", 0, 0)` |
  | `KafkaEx.GenConsumer` | `KafkaEx.Consumer.GenConsumer` |
  | `KafkaEx.ConsumerGroup` | `KafkaEx.Consumer.ConsumerGroup` |
  | `kafka_version: "kayrock"` | (remove — no longer needed) |

### README.md
- [ ] **Update install snippet** (currently `{:kafka_ex, "~> 1.0.0-rc.1"}`) → `{:kafka_ex, "~> 1.0"}`.
- [ ] **Fix `:auto_offset_reset` example** (line ~169). Example shows `:earliest`; code default is `:none`. Pick one: either change default or change example to match reality.
- [ ] **Clarify supported Kafka range**: current text says "0.10.0+". Kayrock requires 0.11+ for RecordBatch/headers. State "Kafka 0.11.0+ required; tested against 2.1.0 through 3.x; 4.x compatibility tracked in #497".
- [ ] Add **"Project status"** section linking to MAINTAINERS.md and being honest about support scope.

### AUTH.md
- [ ] Note that OAUTHBEARER **does not auto-refresh on token expiry** — user must recycle connections. (Already half-documented; tighten wording and link to a recipe.)

---

## P0 — Project sustainability disclosures

- [ ] **Create `MAINTAINERS.md`** naming current maintainer(s) with contact and support scope. Minimum: "Primary maintainer: Piotr Rybarczyk (@Argonus, Fresha). Support scope: critical bugs and Kafka compatibility issues. PRs welcome; response target 2 weeks."
- [ ] **Respond to Issue #497** (Kafka 4.0 JoinGroup incompatibility, open since 2025-08-07). Either fix before the tag or explicitly acknowledge as a known 1.0 limitation with a tracking milestone.
- [ ] **Triage stale open issues** — 29 open, many from 2021–2023 dormancy era. Batch-close or batch-comment before the tag so new users don't see an unresponsive project.
- [ ] **Release framing**: do NOT market as "stable long-term-supported". Frame as "v1.0 — rewritten on Kayrock, seeking co-maintainers." Honest, matches the bus factor.

---

## P1 — Config hygiene (fix or the first contributor will file bugs)

- [ ] **Remove or wire up `:consumer_group_update_interval`** — stored in `Client.State` (`lib/kafka_ex/client/state.ex:52`), documented in README, **never read anywhere else**. Dead config.
- [ ] **Document `:sleep_for_reconnect`** (used in `client.ex:951`, default 400ms) — currently invisible to users tuning reconnect behaviour.
- [ ] **Document `:max_restarts` / `:max_seconds`** (`kafka_ex.ex:150-151`) — supervisor-level knobs not in any config doc.
- [ ] **Legacy `:sasl_username`/`:sasl_password`/`:sasl_mechanism`** keys — either document the fallback in AUTH.md **or** delete them from `auth/config.ex:96-98`. Silent backwards-compat is a future bug source.
- [ ] Ensure `config/config.exs` in repo root reflects the documented defaults (for `mix new` users who copy-paste).
- [x] **Document `:api_versions` app config.** ✅ `ba2a938` — added to `config/config.exs` with a full commented example.

---

## P1 — Release-gate checklist (run right before `hex publish`)

- [ ] `mix format --check-formatted`
- [ ] `mix credo --strict`
- [ ] `mix dialyzer` (0 errors)
- [x] `mix test.unit` — 3182 tests green as of 2026-04-18.
- [ ] `mix test.integration` (against `scripts/docker_up.sh` cluster) — last green run includes the new `consumer_group_rejoin_test.exs`.
- [ ] `mix test.chaos` — last green run includes the new `rejoin_loop_chaos_test.exs`.
- [ ] `mix compile --warnings-as-errors` — **MUST be clean of kayrock's warnings too** (depends on kayrock `:ssl` fix above).
- [ ] `mix hex.build` — inspect generated tarball (no .env, no secrets, no unnecessary files).
- [ ] `mix hex.publish --dry-run`.
- [ ] Verify `mix.exs` `@version` matches git tag.
- [ ] Verify `package` files list in `mix.exs` matches intended files.
- [ ] Lockfile refresh: `mix deps.update --all` on dev/test-only deps (see roadmap.md for list) — don't ship with month-stale lockfile entries.

---

## P1 — Optional (strongly recommended)

- [ ] Add **`Code.ensure_loaded?/1` guards** in `Client.init/1` for features that depend on optional deps. When the user configures `compression: :snappy` without `{:snappyer, ...}` in their app, crash loudly at startup, not at first produce.
- [ ] Add **CONTRIBUTING.md maintainer-response note**: "Typical response time X weeks. Security issues to Y."
- [ ] Add `.github/FUNDING.yml` if interested in sponsorship (signal to community the project has a future).
- [ ] Consider adding an optional `c:handle_commit_failure/3` behaviour callback on `KafkaEx.Consumer.GenConsumer` (parity with brod's `handle_commit_failure/2`, Java's `CommitFailedException`). Currently users can only observe failures via the `[:kafka_ex, :consumer, :commit_failed]` telemetry event — a synchronous callback would be strictly additive.

---

## Go / No-Go summary

**Safe to release when:**
- All P0 checkboxes are green.
- Release notes explicitly acknowledge known gaps (no transactions, no KIP-848, no idempotent producer, Kafka 4.0 compat tracked separately).
- MAINTAINERS.md is in place.
- CI gate on master is green with no kayrock warnings.

**Expect hex-publish to be rejected if:**
- kayrock is still a path dep.
- mix.lock refers to rc versions without `mix.exs` backing.
- `mix.exs` `package.files` includes private files.

**Expect user complaints within 30 days if:**
- Headers breaking change is undocumented.
- ~~`illegal_generation` bug ships.~~ ✅ Fixed in Phase B.
- `auto_offset_reset` docs still show `:earliest`.
- `#497` still hangs open without acknowledgment.

When all of P0 is clean, run the Release-gate checklist once, publish kayrock first, then kafka_ex.
