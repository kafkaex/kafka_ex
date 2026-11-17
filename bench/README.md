# kafka_ex benchmark harness

A standalone Mix project that measures the kafka_ex client against a local Docker Kafka cluster and compares two kafka_ex source trees on the same machine. It is not part of the hex package and the root project does not depend on it.

## Purpose

The harness is the gate for "no regression" and "improvement" claims made by the RFC 0001 refactor PRs. It runs the same scenarios against a baseline tree and a candidate tree, interleaved, and applies a fixed pass/fail rule (see [The rule](#the-rule)).

Absolute numbers from a laptop and a local Docker cluster are not production numbers. Only the comparison between two trees measured on the same machine in the same session means anything.

The harness uses only the public `KafkaEx.API`. Process introspection (`Process.info/2`) is used on the client pid for observation only.

## Prerequisites

- Docker with at least 12 GB of memory for the Docker VM. Brokers were OOM-killed at 7.7 GB; at 16.7 GB peak broker usage was about 1.3 GiB each.
- Elixir 1.14+ and an OTP version supported by kafka_ex.
- The 3-broker cluster plus the Toxiproxy overlay, started from the repository root or from anywhere:

  ```bash
  bench/scripts/cluster_up.sh
  ```

  It picks `docker-compose-arm.yml` or `docker-compose.yml` by CPU architecture, adds `bench/docker/compose.latency.yml`, and waits up to 180 s until all brokers are healthy, the setup containers exited 0 and Toxiproxy answers on port 8474.
- A clean cluster before recording baselines. Leftover topics from earlier work (for example 549 topics after the integration suite) change cold start numbers and add noise. `bench.run` warns when it finds leaked `bench-` topics; delete them before trusting results.
- Nothing else running against the cluster during measurements, and no other heavy load on the machine.
- Dependencies installed once: `cd bench && mix deps.get`.

## Network profiles

| Profile | Ports | Meaning |
|---|---|---|
| `direct` | 9092-9094 (SSL), 9292-9294 (SCRAM) | Client talks to the brokers directly. Informational only. |
| `latency` | 9492-9494 (SSL), 9592-9594 (SCRAM) | Client talks through Toxiproxy with a downstream latency toxic. `BENCH_LATENCY_MS` (default 2) is the added round-trip time in milliseconds. |

Only the `latency` profile gates. This is an owner decision: on `direct` the broker round trip is so short that results are bimodal and noisy (CV of throughput 13-38 % on a settled cluster), while the 2 ms profile is stable (CV 0-4 %). Toxiproxy at 0 ms showed no measurable overhead against `direct`.

## Scheduler settings

All commands that produce numbers run under:

```bash
elixir --erl "+S 8:8 +sbwt none +sbwtdcpu none +sbwtdio none" -S mix bench.run ...
```

`bench.compare` does this for you. A fixed scheduler count and disabled busy-waiting reduce run-to-run variation. macOS cannot bind schedulers to cores (`+sbt db` is "not supported"), so on macOS the report records `scheduler_bind_type: unbound` and the gap stays.

Reports carry: kafka_ex commit sha, OTP and Elixir versions, profile, scheduler count and bind type, CPU architecture and core count, OS name and version, and `harness_dirty` (uncommitted changes in `bench/`, ignoring build output, `deps` and untracked `results/tmp`). They carry no hostname and no user paths.

## Scenarios

Each scenario creates uniquely named topics (`bench-...`) with replication factor 3, balanced leaders across the three brokers, and deletes them afterwards. One started client is shared by N caller processes. Any error, exception, exit or timeout inside a measured job invalidates that run. A job with fewer samples than `min_samples` is invalid as well.

| Scenario | What it measures | Jobs | Default time / warmup (s) | `min_samples` | Gated metrics |
|---|---|---|---|---|---|
| `basic` | Produce (1 KiB, acks=-1) and fetch (64 KiB, max wait 100 ms) at 1, 10 and 100 callers over 6 partitions | `produce_c1`, `produce_c10`, `produce_c100`, `fetch_c1`, `fetch_c10`, `fetch_c100` | 10 / 2 | 500 | p50; throughput (c10, c100); `vm_reductions_per_op` |
| `mixed` | Produce and metadata calls while 3 callers long-poll idle partitions (fetch max wait 100 ms) | `produce_under_longpoll`, `metadata_under_longpoll` | 30 / 2 | 30 | p50 |
| `one_broker` | 100 callers producing to partitions led by one broker: spread over all partitions versus a single partition | `produce_spread_c100`, `produce_single_partition_c100` | 10 / 2 | 500 | p50; throughput; `vm_reductions_per_op` |
| `large_fetch` | 10 callers doing small produces or small fetches while 4 background workers fetch 100 KiB records with `max_bytes` 5 MB | `small_produce_c10`, `small_fetch_c10` | 15 / 2 | 200 | p50; throughput |
| `cold_start` | Time from starting a new client until every partition of a topic with 1, 10 or 100 partitions was fetched once (SCRAM) | `cold_start_p1`, `cold_start_p10`, `cold_start_p100` | one-shot: 15 repetitions, first discarded | 15 | p50 |
| `restart_recovery` | Longest gap between successful produces per partition while the `kafka-2` container is restarted | `restart_kafka2` | one-shot: 3 repetitions | 3 | usable runs only, never the size of the gap (`latency`) |

`restart_recovery` is gated on validity only: on the `latency` profile a restart repetition that does not recover (no successful produce on some partition within the bound) is an unusable run and rules 1-3 below apply to it (3 usable runs per side are required), but the size of the gap is never judged and the gate always reports PASS once the runs are usable. The gap itself stays visible in the gate rows. Two patterns make a gap gate unusable: the client sleeps 500 ms between reconnect attempts inside its GenServer, so recovery gaps come in steps of about 0.5 s and which step a repetition lands on depends on when the restart hits an in-flight request; and on the `latency` profile the restarted leader's partition stalled about 6.2 s (5.7-6.7 s) in 16 of 36 A/A repetitions, always together with produce errors, which looks like a request timeout rather than a 0.5 s reconnect step (the cause is not verified). Per-run medians therefore jump between modes even when both sides run the same tree (A/A). The RFC 0001 improvement, healthy partitions no longer stalling, is far larger than the spacing of the steps and stays visible in the rows.

`restart_recovery` runs last in a comparison because it restarts the `kafka-2` container through `docker compose`. Before every repetition it waits for full ISR; each repetition uses a fresh topic with broker 2 as preferred leader of partition 0. The reported `restart_cmd_*_ms` and gap start/end offsets are measured from the start of the measured repetition window, not from process start. The scenario needs the Docker cluster and the project directory layout of this repository.

Time and warmup are per measured job. Lower `--time` values produce too few samples for slow jobs (for example `mixed` collects about 3 samples per second on the baseline, N = 3 long-polls x 100 ms, because each produce waits behind the long-polls), so `bench.run` refuses a `--time` below the scenario default unless `--allow-short` is given.

## Running one scenario

```bash
cd bench
elixir --erl "+S 8:8 +sbwt none +sbwtdcpu none +sbwtdio none" -S mix bench.run \
  --scenario basic --profile latency --out tmp/basic-latency
```

Switches: `--scenario` (default `basic`), `--profile` (`direct` default, or `latency`), `--security` (`ssl` default, or `scram`), `--runs` (default 5), `--warmup`, `--time`, `--allow-short`, `--out` (required).

Output: `<out>.json` and `<out>.md` (written by this project's own code; benchee `save:`/`load:` is never used because they deserialise Erlang terms).

Defaults are what to use for real measurements. `--allow-short` is for smoke runs only (for example `--time 3 --warmup 1 --runs 1`); such reports are marked short and must not be compared.

One command per scenario and profile, with defaults:

```bash
for scenario in basic mixed one_broker large_fetch cold_start; do
  for profile in latency direct; do
    elixir --erl "+S 8:8 +sbwt none +sbwtdcpu none +sbwtdio none" -S mix bench.run \
      --scenario $scenario --profile $profile --out tmp/$scenario-$profile
  done
done
elixir --erl "+S 8:8 +sbwt none +sbwtdcpu none +sbwtdio none" -S mix bench.run \
  --scenario restart_recovery --profile latency --out tmp/restart_recovery-latency
```

`restart_recovery` also runs on `direct`; there, like every `direct` cell, it is informational.

Two kafka_ex trees must not share a build directory. `KAFKA_EX_PATH` selects the kafka_ex source tree (default `..`, the repository root) and `BENCH_BUILD_PATH` the Mix build path (default `_build`):

```bash
KAFKA_EX_PATH=/path/to/candidate BENCH_BUILD_PATH=_build_candidate \
  elixir --erl "+S 8:8 +sbwt none +sbwtdcpu none +sbwtdio none" -S mix bench.run --scenario basic --profile latency --out tmp/cand
```

## Comparing two trees

```bash
cd bench
mix bench.compare --base <rev-or-dir> --candidate <rev-or-dir> --out tmp/compare
```

`--base` and `--candidate` accept a directory (a kafka_ex checkout) or a commit-ish. A commit-ish is checked out into a detached git worktree under `<out>/worktrees` and removed afterwards. A directory with uncommitted changes in `lib`, `config`, `mix.exs` or `mix.lock` is refused unless `--allow-dirty`.

| Switch | Meaning |
|---|---|
| `--scenarios a,b` | Subset of scenarios (default: all six) |
| `--profiles latency,direct` | Profiles (default: both) |
| `--runs N` | Runs per side per scenario and profile (default 6; five usable runs are needed, so one bad run is tolerated) |
| `--threshold PCT` | Relative threshold (default 5.0) |
| `--min-valid N` | Override the per-scenario minimum of usable runs (at least 2) |
| `--candidate-env KEY=VAL` | Environment variable for the candidate side only; repeatable |
| `--allow-ungated` | Exit 0 when nothing was gated (informational run) |
| `--time S`, `--allow-short` | Pass `--time` to every `bench.run`; short times need `--allow-short` and give a smoke run only |
| `--out DIR` | Output directory (required) |

Each run is one `bench.run --runs 1` process under the scheduler flags above, with its own `KAFKA_EX_PATH` and `_build_<side>` directory.

Order: for each profile and scenario the pair order alternates by run index (base then candidate on odd runs, candidate then base on even runs: ABBA ABBA ...) so that drift in the machine or cluster and the position within a pair hit both sides equally. All `restart_recovery` runs come after every other scenario.

Outputs in `<out>`: `compare.md` (verdict report), `compare.json`, and `runs/<side>-<profile>-<scenario>-<n>.json|md`. Before any measured run, `mix compile` runs once per side with that side's `KAFKA_EX_PATH` and `BENCH_BUILD_PATH`; a failing build aborts the comparison and the last 40 lines of its output go into the partial report and onto stdout. A single run that exits non-zero or leaves no report is recorded as a failed run and counts as unusable for its side; the comparison aborts when the same side has two consecutive failed runs, when the cluster is found unhealthy, or when leaked `bench-` topics are found. On an abort a partial report is still written and the command exits non-zero.

Both sides build against `bench/mix.lock`: the candidate's own dependency versions are not used. The environment table in `compare.md` shows the dependency versions per side.

Exit status:

| Overall verdict | Exit |
|---|---|
| pass, improvement | 0 |
| fail, inconclusive | 1 |
| not gated (no gated cell: no `latency` cell) | 1, or 0 with `--allow-ungated` |
| abort | non-zero, with a partial report |

A full comparison (6 runs per side, both profiles) takes roughly 100 minutes.

## The rule

Evaluated per gated metric of every job in every gated cell (`latency` profile), then reduced to the worst result.

1. Only usable runs count: a run is usable when it is valid and has a sample for the metric. The minimum is `min_valid` usable runs per side: 5, and 3 for `restart_recovery`. Informational tables show the usable counts too.
2. Baseline with fewer than `min_valid` usable runs: INCONCLUSIVE. Candidate with fewer than `min_valid` usable runs: FAIL.
3. Candidate with more unusable runs than the baseline: INCONCLUSIVE.
4. Relative gates: the value per run is the run's median; the comparison uses the median across runs. `delta` is the candidate's change in the bad direction, as a percentage of the baseline median.
5. `noise` = max(CV of baseline runs, CV of candidate runs); the gate table also prints the CV of each side. Noise above 10 %: INCONCLUSIVE.
6. `limit` = max(`threshold`, 2 x `noise`). `threshold` is the gate's own value when its declaration is `{:relative, pct}`, otherwise the `--threshold` switch (default 5 %). All current gates are `{:relative}`; per-class values are pending the A/A run and are set by the owner.
7. Worse by more than `limit`: FAIL. Better by more than the same `limit`: IMPROVEMENT. Anything else: PASS.
8. Usable-only gates (`restart_recovery` gap metrics): after rules 1-3 the result is PASS with the reason "usable runs only, value not judged" and an `n/a` limit; the value is never compared.
9. Absolute gates (supported by the rule, none declared at present; `restart_recovery` gaps were the only users): FAIL when the candidate median is worse than the baseline median by more than 250 ms, IMPROVEMENT when better by more than 250 ms, otherwise PASS. If within either side the range (max - min) of the per-run values exceeds 250 ms, the result is INCONCLUSIVE (run-to-run spread too large for the limit). The percentage noise is reported, not used.
10. A job with no gates (empty gate list) is INCONCLUSIVE, so a scenario cannot pass without being judged.
11. Cell and overall verdict: the worst job result, ordered FAIL, INCONCLUSIVE, PASS, IMPROVEMENT.

The 5 % default threshold is provisional. The owner decides the final value after the A/A run (the same tree on both sides), whose noise floor shows how small a real change can be detected; the final choice is recorded in the tracker, not here.

### Reruns

The overall verdict is the worst of 31 steady-state latency gates (basic 16, mixed 2, one_broker 6, large_fetch 4, cold_start 3) and the two restart validity rows, so one noisy gate can fail or void a comparison. A FAIL counts only if it reproduces when that scenario is rerun with `mix bench.compare --scenarios <scenario>`. INCONCLUSIVE means rerun on a quieter machine (close other containers and apps) or with more `--runs`. Never drop individual runs and never merge runs from different compares.

### Noise floor (A/A, 2026-11-16)

`results/2026-11-16-aa.md` compares the same tree on both sides. The latency-profile gates passed 31 of 31 (plus the two restart rows), the largest |delta| was 3.06 %, the largest noise 6.45 % (`large_fetch`), and the noise of the reductions per operation was at most 0.18 %. The file's overall line says INCONCLUSIVE because it was produced while `restart_recovery` still carried absolute gates; keep that in mind when reading it.

`results/2026-11-17-sensitivity.md` shows the other side: the candidate ran with `BENCH_LATENCY_MS=3` against 2 for the base (`basic` and `mixed`, latency profile, 3 runs). Every p50 and throughput gate in `basic` FAILED (p50 +34 % to +45 %, throughput -24 % to -29 %), every `vm_reductions_per_op` gate PASSED (largest +0.77 %), and `mixed` p50 moved +1.6 % at 0.08 % noise and PASSED under the 5 % threshold, so a change that small is below what the threshold can resolve.

## Informational metrics

Reported in `compare.md` and the run reports but never gated:

Metrics whose direction is known (declared by the gates or the job's `metrics`) show "worse Δ %", where a positive value is worse; the others show the raw candidate-minus-base "raw Δ %". Per cell, `background_ops` and `rejoin_ms` (median of the per-run values) and `errors_during_repetitions` and `fetch_retries` (sum over runs) are listed as extras when a job reports them.

- `client_reductions_per_op`: reductions of the client process per operation. The RFC 0001 refactor moves work between processes by design, so this number is expected to change.
- `vm_cpu_us_per_op`: CPU time of the whole VM per operation. Includes the harness, the sockets and the schedulers' own overhead, so it is noisy. `vm_reductions_per_op` is whole-VM as well; it is gated only in `basic` and `one_broker`, where the load is uniform.
- `p99_us`: at 10 and 100 callers p99 is roughly queue depth multiplied by service time of the single client process, so it follows throughput, not tail behaviour.
- `max_mailbox`: peak mailbox length of the client process (equals the number of callers when the client is the bottleneck).
- `background_ops`: operations done by background workers (large fetches in `large_fetch`); a large change means the background load differed between sides.
- `rejoin_ms` (`restart_recovery`): time until the restarted broker is back, a property of Kafka and Docker.
- On `direct`, every metric is informational.

## Known limits

- One client process serializes all requests, so a long-poll that the broker answers early (data arrived) is hidden behind earlier requests. The harness cannot show that benefit until the client stops serializing.
- `restart_recovery` polls cluster readiness with the measured client, so a client defect can stretch the readiness waits.
- VM metrics cover the whole Erlang VM, including the harness itself.
- Results are machine-specific. Never compare stored numbers across machines, Docker settings or even sessions with different background load; always run both sides in one `bench.compare`.
- macOS cannot bind schedulers (see above).
- A local cluster has one machine's CPU shared by three brokers, Toxiproxy and the client.

## What the baseline shows today

`large_fetch` gates run with noise of 5-6.5 % on `latency` (8.7-10.2 % on `direct`), so their limit widens to 10-13 % and an occasional INCONCLUSIVE is expected.

Indicative local numbers from one machine (macOS arm64, 18 logical cores, Docker VM 16.7 GB), unmodified kafka_ex 1.2.0 code. They show where the refactor is expected to help; they are not a promise.

- `basic`, `latency` (2 ms): produce throughput stays at about 300 ops/s for 1, 10 and 100 callers (about 1 / RTT) and p50 grows from 3.3 ms to 32 ms to 336 ms. The client mailbox length equals the number of callers, so the single client process is the limiter, not the brokers.
- `mixed`: produce and metadata p50 is about 309-320 ms with 3 long-polls running, roughly the number of long-polls multiplied by the 100 ms max wait. The client mailbox holds 3-4 messages.
- `large_fetch`: small produces and fetches have p50 85-131 ms while 4 workers fetch large batches, because they wait behind the large fetches; throughput is about 75-115 ops/s.
- `restart_recovery`: after restarting `kafka-2`, partitions whose leaders did not move stall for about 1.5 s in one of the modes, longer than the partition led by the restarted broker in that mode (about 1.0 s), a sign of head-of-line blocking in the single client process; the other mode is the roughly 6.2 s stall described above.
- `cold_start`, `latency`: first fetch on all partitions takes about 65 ms for 1 partition and 370 ms for 100 partitions (direct: about 18 ms and 50 ms).
