# Kafka API Version Upgrade Tracker

Track implementation of new Kayrock-supported API versions in KafkaEx.

## Legend

| Symbol | Meaning                                       |
|--------|-----------------------------------------------|
| 🟢     | Implemented and tested                        |
| 🟡     | In progress                                   |
| ⬜     | Not started                                   |
| ⏭️     | Intentionally skipped (no meaningful changes) |
| FLEX   | Flexible version (compact encodings + tagged_fields) |

**Test columns:** Unit = protocol-layer unit tests, Integ = integration tests (live broker), Chaos = chaos/fault-injection tests.

---

## 1. ApiVersions (API Key 18)

**Current:** V0-V3 | **Available:** V0-V3

> **Note:** Integration/chaos tests skipped (⏭️) — ApiVersions is implicitly exercised by every other integration and chaos test since it's the first request sent on every broker connection.

| Version | Status  | Request Changes                                                             | Response Changes                       | Effort | Unit    | Integ    | Chaos |
|---------|---------|-----------------------------------------------------------------------------|----------------------------------------|--------|---------|----------|-------|
| V0      | 🟢      | —                                                                           | —                                      | —      | 🟢      | ⏭️       | ⏭️    |
| V1      | 🟢      | —                                                                           | —                                      | —      | 🟢      | ⏭️       | ⏭️    |
| V2      | 🟢      | No changes vs V1                                                            | No changes vs V1                       | Low    | 🟢      | ⏭️        | ⏭️    |
| V3      | 🟢      | FLEX: +`client_software_name`, +`client_software_version`, +`tagged_fields` | FLEX: +`tagged_fields`, compact arrays | Medium | 🟢      | ⏭️        | ⏭️    |

---

## 2. Metadata (API Key 3)

**Current:** V0-V9 (all explicit) | **Available:** V0-V9

| Version | Status | Request Changes                                  | Response Changes                               | Effort | Unit | Integ | Chaos |
|---------|--------|--------------------------------------------------|------------------------------------------------|--------|------|-------|-------|
| V0      | 🟢     | —                                                | —                                              | —      | 🟢   | 🟢    | ⏭️    |
| V1      | 🟢     | —                                                | —                                              | —      | 🟢   | 🟢    | ⏭️    |
| V2      | 🟢     | —                                                | —                                              | —      | 🟢   | 🟢    | ⏭️    |
| V3      | 🟢     | No changes vs V2                                 | +`throttle_time_ms`, +`cluster_id`             | Low    | 🟢   | ⏭️    | ⏭️    |
| V4      | 🟢     | +`allow_auto_topic_creation`                     | No changes vs V3                               | Low    | 🟢   | ⏭️    | ⏭️    |
| V5      | 🟢     | No changes vs V4                                 | +`offline_replicas` in partitions              | Low    | 🟢   | ⏭️    | ⏭️    |
| V6      | 🟢     | No changes vs V5                                 | No changes vs V5                               | Low    | 🟢   | ⏭️    | ⏭️    |
| V7      | 🟢     | No changes vs V6                                 | +`leader_epoch` in partitions                  | Low    | 🟢   | ⏭️    | ⏭️    |
| V8      | 🟢     | +`include_cluster/topic_authorized_operations`   | +`cluster/topic_authorized_operations`         | Medium | 🟢   | ⏭️    | ⏭️    |
| V9      | 🟢     | FLEX: compact arrays/strings, +`tagged_fields`   | FLEX: compact arrays/strings, +`tagged_fields` | Medium | 🟢   | ⏭️    | ⏭️    |

> **Note:** `Any` fallback retained for forward compatibility with unknown future versions. All V0-V9 have explicit `defimpl` impls.

---

## 3. Produce (API Key 0)

**Current:** V0-V8 (all explicit) | **Available:** V0-V8

| Version | Status | Request Changes                         | Response Changes                                                | Effort | Unit | Integ | Chaos |
|---------|--------|-----------------------------------------|-----------------------------------------------------------------|--------|------|-------|-------|
| V0      | 🟢     | —                                       | —                                                               | —      | 🟢   | 🟢    | 🟢    |
| V1      | 🟢     | —                                       | +`throttle_time_ms`                                             | —      | 🟢   | 🟢    | 🟢    |
| V2      | 🟢     | —                                       | +`log_append_time`                                              | —      | 🟢   | 🟢    | 🟢    |
| V3      | 🟢     | +`transactional_id`, RecordBatch format | Same as V2                                                      | —      | 🟢   | 🟢    | 🟢    |
| V4      | 🟢     | No changes vs V3                        | Same as V3                                                      | —      | 🟢   | 🟢    | 🟢    |
| V5      | 🟢     | No changes vs V4                        | +`log_start_offset`                                             | —      | 🟢   | 🟢    | 🟢    |
| V6      | 🟢     | No changes vs V5                        | No changes vs V5                                                | Low    | 🟢   | ⏭️    | ⏭️    |
| V7      | 🟢     | No changes vs V6                        | No changes vs V6                                                | Low    | 🟢   | ⏭️    | ⏭️    |
| V8      | 🟢     | No changes vs V7                        | +`record_errors` array, +`error_message` in partition_responses | Medium | 🟢   | ⏭️    | ⏭️    |

> **Note:** `Any` fallback retained for forward compatibility with unknown future versions. All V0-V8 have explicit `defimpl` impls. V8 `record_errors` and `error_message` fields are parsed by Kayrock but not currently exposed in `RecordMetadata` domain struct -- they are only populated in error scenarios and the error path handles them via the standard error code mechanism.
>
> **Integration/chaos tests skipped (⏭️) for V6-V8:** These are pure delegation layers — all request impls call the same `build_request_v3_plus/2` helper, all response impls use the same field extractor as V5. Default produce version is V3 (`@default_api_version[:produce]` = 3), so V6-V8 are only used when explicitly requested. Existing V0-V5 integration tests already cover the full produce path end-to-end. Chaos tests are version-independent (broker failures affect all versions identically). Would revisit if: default version bumped to 6+, V8 `record_errors` exposed in domain structs, or flexible versions (V9+) added.

---

## 4. Fetch (API Key 1)

**Current:** V0-V11 | **Available:** V0-V11

| Version | Status | Request Changes                          | Response Changes                   | Effort | Unit | Integ | Chaos |
|---------|--------|------------------------------------------|------------------------------------|--------|------|-------|-------|
| V0-V7   | 🟢     | —                                        | —                                  | —      | 🟢   | 🟢    | 🟢    |
| V8      | 🟢     | No changes vs V7                         | No changes vs V7                   | Low    | 🟢   | 🟢    | ⏭️    |
| V9      | 🟢     | +`current_leader_epoch` in partitions    | No changes vs V8                   | Low    | 🟢   | 🟢    | ⏭️    |
| V10     | 🟢     | No changes vs V9                         | No changes vs V9                   | Low    | 🟢   | 🟢    | ⏭️    |
| V11     | 🟢     | +`rack_id` (top-level)                   | +`preferred_read_replica` per part | Low    | 🟢   | 🟢    | ⏭️    |

> **Chaos tests skipped (⏭️) for V8-V11:** These are pure delegation layers — all request impls call `build_request_v7_plus/3`, all response impls use the same field extractor as V5+ (V11 adds `extract_v11_fields/2` but only for `preferred_read_replica`). New fields (`current_leader_epoch`, `rack_id`, `preferred_read_replica`) use safe defaults and do not affect error handling or reconnection behavior. Existing chaos tests at `test/chaos/consumer_test.exs` exercise the fetch error path which is shared across all versions. Would revisit if: default version bumped to V9+, epoch-aware/rack-aware fetch implemented, or multi-broker chaos infrastructure added.

---

## 5. ListOffsets (API Key 2)

**Current:** V0-V5 (all explicit) | **Available:** V0-V5

| Version | Status | Request Changes                          | Response Changes                  | Effort | Unit | Integ | Chaos |
|---------|--------|------------------------------------------|-----------------------------------|--------|------|-------|-------|
| V0-V2   | 🟢     | —                                        | —                                 | —      | 🟢   | 🟢    | ⏭️     |
| V3      | 🟢     | +`current_leader_epoch` in partitions    | No changes vs V2                  | Low    | 🟢   | ⏭️    | ⏭️     |
| V4      | 🟢     | No changes vs V3                         | +`leader_epoch` in partitions     | Low    | 🟢   | ⏭️    | ⏭️     |
| V5      | 🟢     | No changes vs V4                         | No changes vs V4                  | Low    | 🟢   | ⏭️    | ⏭️     |

> **Note:** `Any` fallback retained for forward compatibility with unknown future versions. All V0-V5 have explicit `defimpl` impls. V2-V5 request impls all delegate to `RequestHelpers.build_request_v2_plus/3` -- V2/V3 build partitions without `current_leader_epoch`, V4/V5 include it. V3 response uses same extractor as V2. V4/V5 response uses `extract_v4_offset/2` which parses `leader_epoch` (not yet exposed in `PartitionOffset` domain struct). Kayrock V3 request schema does not include `current_leader_epoch` (it first appears in Kayrock V4).
>
> **Chaos tests skipped (⏭️) for V0-V5:** ListOffsets is a read-only, stateless query with no idempotency or ordering concerns. The client error path (`GenServer.call` → network layer → reconnection) is shared with all other APIs and already chaos-tested via `test/chaos/consumer_test.exs` and `test/chaos/network_test.exs`. ListOffsets is also implicitly exercised by consumer chaos tests (offset lookups are part of the fetch flow). A failed ListOffsets can simply be retried with no side effects. Would revisit if: ListOffsets gains stateful behavior or version-specific error handling.
>
> **Integration/chaos tests skipped (⏭️) for V3-V5:** These are pure delegation layers -- all request impls call the same `RequestHelpers.build_request_v2_plus/3` helper, all response impls use the same field extractors (`extract_v2_offset/2` for V3, `extract_v4_offset/2` for V4/V5). Default ListOffsets version is V1 (`@default_api_version[:list_offsets]` = 1), so V3-V5 are only used when explicitly requested. Existing V0-V2 integration tests already cover the full ListOffsets path end-to-end. New fields (`current_leader_epoch`, `leader_epoch`) use safe defaults (-1) and do not affect error handling or reconnection behavior. Chaos tests are version-independent (broker failures affect all versions identically). Would revisit if: default version bumped to V3+, `leader_epoch` exposed in domain structs, or flexible versions (V6+) added.

---

## 6. OffsetFetch (API Key 9)

**Current:** V0-V6 (all explicit) | **Available:** V0-V6

| Version | Status | Request Changes                          | Response Changes                                        | Effort | Unit | Integ | Chaos |
|---------|--------|------------------------------------------|---------------------------------------------------------|--------|------|-------|-------|
| V0-V3   | 🟢     | —                                        | —                                                       | —      | 🟢   | 🟢    | ⬜    |
| V4      | 🟢     | No changes vs V3                         | No changes vs V3                                        | Low    | 🟢   | ⏭️    | ⏭️    |
| V5      | 🟢     | No changes vs V4                         | +`committed_leader_epoch` per partition                 | Low    | 🟢   | ⏭️    | ⏭️    |
| V6      | 🟢     | FLEX: +`tagged_fields`, compact types    | FLEX: +`tagged_fields`, compact types + leader_epoch    | Medium | 🟢   | ⏭️    | ⏭️    |

> **Note:** `Any` fallback retained for forward compatibility with unknown future versions. All V0-V6 have explicit `defimpl` impls. All request impls use the same logic (group_id + topics). V4 response is identical to V3 (delegates to `parse_response_with_top_level_error`). V5+ response adds `committed_leader_epoch` per partition, mapped to `leader_epoch` in `PartitionOffset` domain struct via `parse_response_with_leader_epoch`. V6 is a flexible version (KIP-482) -- Kayrock handles compact encoding/decoding transparently.
>
> **Integration/chaos tests skipped for V4-V6:** These are pure delegation layers -- all request impls call the same `RequestHelpers.build_topics/1` + `extract_common_fields/1` pattern, V4 response delegates to same helper as V2/V3, V5/V6 response delegates to `parse_response_with_leader_epoch`. Default OffsetFetch version is determined by version negotiation. Existing V0-V3 integration tests already cover the full OffsetFetch path end-to-end. New field (`committed_leader_epoch`) uses safe default and is mapped to existing `leader_epoch` field in `PartitionOffset`. Would revisit if: default version bumped, `leader_epoch` used for fencing logic, or flexible version encoding issues discovered.

---

## 7. OffsetCommit (API Key 8)

**Current:** V0-V8 (all explicit) | **Available:** V0-V8

| Version | Status | Request Changes                             | Response Changes                         | Effort | Unit | Integ | Chaos |
|---------|--------|---------------------------------------------|------------------------------------------|--------|------|-------|-------|
| V0-V3   | 🟢     | —                                           | —                                        | —      | 🟢   | 🟢    | ⬜    |
| V4      | 🟢     | No changes vs V3                            | No changes vs V3                         | Low    | 🟢   | ⏭️    | ⏭️    |
| V5      | 🟢     | -`retention_time_ms` removed                | No changes vs V4                         | Low    | 🟢   | ⏭️    | ⏭️    |
| V6      | 🟢     | +`committed_leader_epoch` in partitions     | No changes vs V5                         | Low    | 🟢   | ⏭️    | ⏭️    |
| V7      | 🟢     | +`group_instance_id`                        | No changes vs V6                         | Low    | 🟢   | ⏭️    | ⏭️    |
| V8      | 🟢     | FLEX: +`tagged_fields`, compact types       | FLEX: +`tagged_fields`, compact types    | Medium | 🟢   | ⏭️    | ⏭️    |

> **Note:** `Any` fallback retained for forward compatibility with unknown future versions. All V0-V8 have explicit `defimpl` impls. V4 request delegates to same `build_v2_v3_request` as V2/V3 (schema-identical). V5 removes `retention_time_ms` (offset retention now broker-controlled). V6 adds `committed_leader_epoch` per partition. V7 adds `group_instance_id` for static membership (KIP-345). V8 is the flexible version (KIP-482) -- Kayrock handles compact encoding transparently. All V4-V8 responses share the same structure (throttle_time_ms + topics with partition_index/error_code) and delegate to the same `ResponseHelpers.parse_response/1`.
>
> **Integration/chaos tests skipped (⏭️) for V4-V8:** These are pure delegation layers -- V4 request is schema-identical to V3, V5-V8 request impls each delegate to version-specific helpers but all use the same `build_topics`/`build_topics_with_leader_epoch` pattern. All V4-V8 responses delegate to the same `ResponseHelpers.parse_response/1`. Default OffsetCommit version is determined by version negotiation. Existing V0-V3 integration tests already cover the full OffsetCommit path end-to-end. New fields (`committed_leader_epoch`, `group_instance_id`) use safe defaults and do not affect error handling or reconnection behavior. Would revisit if: default version bumped, `committed_leader_epoch` used for fencing, `group_instance_id` integrated with consumer group logic, or flexible version encoding issues discovered.

---

## 8. FindCoordinator (API Key 10)

**Current:** V0-V3 (all explicit) | **Available:** V0-V3

| Version | Status | Request Changes                             | Response Changes                            | Effort | Unit | Integ | Chaos |
|---------|--------|---------------------------------------------|---------------------------------------------|--------|------|-------|-------|
| V0-V1   | 🟢     | —                                           | —                                           | —      | 🟢   | 🟢    | ⬜    |
| V2      | 🟢     | No changes vs V1                            | No changes vs V1                            | Low    | 🟢   | ⏭️    | ⏭️    |
| V3      | 🟢     | FLEX: +`tagged_fields`, compact strings     | FLEX: +`tagged_fields`, compact strings     | Medium | 🟢   | ⏭️    | ⏭️    |

> **Note:** `Any` fallback retained for forward compatibility with unknown future versions. All V0-V3 have explicit `defimpl` impls. V2 is schema-identical to V1 (pure version bump). V3 is the flexible version (KIP-482) -- Kayrock handles compact encoding/decoding transparently, domain-relevant fields are identical to V1/V2. All V1+ request impls delegate to `RequestHelpers.extract_v1_fields/1`. All V1+ response impls delegate to `ResponseHelpers.parse_v1_response/1`.
>
> **Integration/chaos tests skipped (⏭️) for V2-V3:** These are pure delegation layers -- V2 request/response are schema-identical to V1, V3 uses compact encoding but Kayrock handles this transparently. Default FindCoordinator version is determined by version negotiation. Existing V0-V1 integration tests already cover the full FindCoordinator path end-to-end. Would revisit if: default version bumped, or flexible version encoding issues discovered.

---

## 9. JoinGroup (API Key 11)

**Current:** V0-V6 (all explicit) | **Available:** V0-V6

| Version | Status | Request Changes                          | Response Changes                            | Effort | Unit | Integ | Chaos |
|---------|--------|------------------------------------------|---------------------------------------------|--------|------|-------|-------|
| V0-V2   | 🟢     | —                                        | —                                           | —      | 🟢   | 🟢    | ⬜    |
| V3      | 🟢     | No changes vs V2                         | No changes vs V2                            | Low    | 🟢   | ⏭️    | ⏭️    |
| V4      | 🟢     | No changes vs V3                         | No changes vs V3                            | Low    | 🟢   | ⏭️    | ⏭️    |
| V5      | 🟢     | +`group_instance_id`                     | +`group_instance_id` in members             | Low    | 🟢   | ⏭️    | ⏭️    |
| V6      | 🟢     | FLEX: +`tagged_fields`, compact types    | FLEX: +`tagged_fields`, compact types       | Medium | 🟢   | ⏭️    | ⏭️    |

> **Note:** `Any` fallback retained for forward compatibility with unknown future versions. All V0-V6 have explicit `defimpl` impls. V3/V4 request schemas are identical to V2 (pure version bumps) — all delegate to `RequestHelpers.build_v1_or_v2_request/2`. V5/V6 request impls delegate to `RequestHelpers.build_v5_plus_request/2` which adds `group_instance_id` (defaults to `nil` for dynamic membership). All V2+ response impls delegate to `ResponseHelpers.parse_response/2` with throttle_time_ms extractor. V5/V6 responses include `group_instance_id` per member in Kayrock schema but this is NOT extracted to the domain layer (`JoinGroup.Member` struct only has `member_id` and `member_metadata`) — same pattern as `leader_epoch` in other APIs. V6 is the flexible version (KIP-482) — Kayrock handles compact encoding/decoding transparently.
>
> **Integration/chaos tests skipped (⏭️) for V3-V6:** These are pure delegation layers — V3/V4 request/response are schema-identical to V2, V5/V6 request impls add `group_instance_id` but delegate to the same helper pattern. All responses use the same `ResponseHelpers.parse_response/2`. Default JoinGroup version is determined by version negotiation. Existing V0-V2 integration tests already cover the full JoinGroup path end-to-end. New field (`group_instance_id`) uses safe default (`nil`) and does not affect error handling or reconnection behavior. Chaos tests are version-independent (broker failures affect all versions identically). Would revisit if: default version bumped, `group_instance_id` integrated with consumer group static membership logic, or flexible version encoding issues discovered.

---

## 10. SyncGroup (API Key 14)

**Current:** V0-V1 | **Available:** V0-V4

| Version | Status                | Request Changes                          | Response Changes                            | Effort  | Unit                  | Integ                 | Chaos                 |
|---------|-----------------------|------------------------------------------|---------------------------------------------|---------|---------------------- |-----------------------|-----------------------|
| V0-V1   | 🟢    | —                                        | —                                           | —       | 🟢    | 🟢    | ⬜ |
| V2      | ⬜ | No changes vs V1                         | No changes vs V1                            | Low     | ⬜ | ⬜ | ⬜ |
| V3      | ⬜ | +`group_instance_id`                     | +`protocol_type`, +`protocol_name`          | Low-Med | ⬜ | ⬜ | ⬜ |
| V4      | ⬜ | FLEX: +`tagged_fields`, compact types    | FLEX: +`tagged_fields`, compact types       | Medium  | ⬜ | ⬜ | ⬜ |

---

## 11. Heartbeat (API Key 12)

**Current:** V0-V1 | **Available:** V0-V4

| Version | Status                | Request Changes                          | Response Changes                         | Effort | Unit                  | Integ                 | Chaos                 |
|---------|-----------------------|------------------------------------------|------------------------------------------|--------|-----------------------|-----------------------|-----------------------|
| V0-V1   | 🟢    | —                                        | —                                        | —      | 🟢    | 🟢    | ⬜ |
| V2      | ⬜ | No changes vs V1                         | No changes vs V1                         | Low    | ⬜ | ⬜ | ⬜ |
| V3      | ⬜ | +`group_instance_id`                     | No changes vs V2                         | Low    | ⬜ | ⬜ | ⬜ |
| V4      | ⬜ | FLEX: +`tagged_fields`, compact types    | FLEX: +`tagged_fields`, compact types    | Medium | ⬜ | ⬜ | ⬜ |

---

## 12. LeaveGroup (API Key 13)

**Current:** V0-V1 | **Available:** V0-V4

| Version | Status                | Request Changes                                                         | Response Changes                          | Effort | Unit                  | Integ                 | Chaos                 |
|---------|-----------------------|-------------------------------------------------------------------------|-------------------------------------------|--------|-----------------------|-----------------------|-----------------------|
| V0-V1   | 🟢    | —                                                                       | —                                         | —      | 🟢    | 🟢    | ⬜ |
| V2      | ⬜ | No changes vs V1                                                        | No changes vs V1                          | Low    | ⬜ | ⬜ | ⬜ |
| V3      | ⬜ | **BREAKING:** -`member_id` -> +`members` array (batch leave, KIP-345)  | +`members` array with per-member errors   | High   | ⬜ | ⬜ | ⬜ |
| V4      | ⬜ | FLEX: +`tagged_fields`, compact types                                   | FLEX: +`tagged_fields`, compact types     | Medium | ⬜ | ⬜ | ⬜ |

---

## 13. DescribeGroups (API Key 15)

**Current:** V0-V1 | **Available:** V0-V5

| Version | Status                | Request Changes                          | Response Changes                          | Effort | Unit                  | Integ                 | Chaos                 |
|---------|-----------------------|------------------------------------------|-------------------------------------------|--------|-----------------------|-----------------------|-----------------------|
| V0-V1   | 🟢    | —                                        | —                                         | —      | 🟢    | 🟢    | ⬜ |
| V2      | ⬜ | No changes vs V1                         | No changes vs V1                          | Low    | ⬜ | ⬜ | ⬜ |
| V3      | ⬜ | +`include_authorized_operations`         | +`authorized_operations` in groups        | Low    | ⬜ | ⬜ | ⬜ |
| V4      | ⬜ | No changes vs V3                         | No changes vs V3                          | Low    | ⬜ | ⬜ | ⬜ |
| V5      | ⬜ | FLEX: +`tagged_fields`, compact types    | FLEX: +`tagged_fields`, compact types     | Medium | ⬜ | ⬜ | ⬜ |

---

## 14. CreateTopics (API Key 19)

**Current:** V0-V2 | **Available:** V0-V5

| Version | Status                | Request Changes                          | Response Changes                                                                       | Effort      | Unit                  | Integ                 | Chaos                 |
|---------|-----------------------|------------------------------------------|----------------------------------------------------------------------------------------|-------------|-----------------------|-----------------------|-----------------------|
| V0-V2   | 🟢    | —                                        | —                                                                                      | —           | 🟢    | 🟢    | ⬜ |
| V3      | ⬜ | No changes vs V2                         | No changes vs V2                                                                       | Low         | ⬜ | ⬜ | ⬜ |
| V4      | ⬜ | No changes vs V3                         | No changes vs V3                                                                       | Low         | ⬜ | ⬜ | ⬜ |
| V5      | ⬜ | FLEX: +`tagged_fields`, compact types    | FLEX: +`num_partitions`, +`replication_factor`, +`configs` array, +`tagged_fields`     | Medium-High | ⬜ | ⬜ | ⬜ |

---

## 15. DeleteTopics (API Key 20)

**Current:** V0-V1 | **Available:** V0-V4

| Version | Status                | Request Changes                          | Response Changes                         | Effort | Unit                  | Integ                 | Chaos                 |
|---------|-----------------------|------------------------------------------|------------------------------------------|--------|-----------------------|-----------------------|-----------------------|
| V0-V1   | 🟢    | —                                        | —                                        | —      | 🟢    | 🟢    | ⬜ |
| V2      | ⬜ | No changes vs V1                         | No changes vs V1                         | Low    | ⬜ | ⬜ | ⬜ |
| V3      | ⬜ | No changes vs V2                         | No changes vs V2                         | Low    | ⬜ | ⬜ | ⬜ |
| V4      | ⬜ | FLEX: +`tagged_fields`, compact types    | FLEX: +`tagged_fields`, compact types    | Medium | ⬜ | ⬜ | ⬜ |

---

## Implementation Order

Prioritized by: (1) most commonly used APIs first, (2) low-effort versions first within each API, (3) group related versions together.

| #  | API             | Version | Effort      | Unit                  | Integ                 | Chaos                 | Notes                              |
|----|-----------------|---------|-------------|-----------------------|-----------------------|-----------------------|------------------------------------|
| 1  | Fetch           | V8      | Low         | 🟢 | 🟢 | ⏭️ | No changes, just wire through      |
 | 2  | Fetch           | V9      | Low         | 🟢 | 🟢 | ⏭️ | +current_leader_epoch              |
 | 3  | Fetch           | V10     | Low         | 🟢 | 🟢 | ⏭️ | No changes                         |
 | 4  | Fetch           | V11     | Low         | 🟢 | 🟢 | ⏭️ | +rack_id                           |
| 5  | Produce         | V6      | Low         | 🟢 | ⏭️ | ⏭️ | No changes                         |
| 6  | Produce         | V7      | Low         | 🟢 | ⏭️ | ⏭️ | No changes                         |
| 7  | Produce         | V8      | Medium      | 🟢 | ⏭️ | ⏭️ | +record_errors in response         |
| 8  | ListOffsets     | V3      | Low         | 🟢 | ⏭️ | ⏭️ | +current_leader_epoch              |
| 9  | ListOffsets     | V4      | Low         | 🟢 | ⏭️ | ⏭️ | +leader_epoch in response          |
| 10 | ListOffsets     | V5      | Low         | 🟢 | ⏭️ | ⏭️ | No changes                          |
| 11 | FindCoordinator | V2      | Low         | 🟢 | ⏭️ | ⏭️ | No changes                         |
| 12 | FindCoordinator | V3      | Medium      | 🟢 | ⏭️ | ⏭️ | FLEX                               |
| 13 | Heartbeat       | V2      | Low         | ⬜ | ⬜ | ⬜ | No changes                         |
| 14 | Heartbeat       | V3      | Low         | ⬜ | ⬜ | ⬜ | +group_instance_id                 |
| 15 | Heartbeat       | V4      | Medium      | ⬜ | ⬜ | ⬜ | FLEX                               |
| 16 | JoinGroup       | V3      | Low         | 🟢 | ⏭️ | ⏭️ | No changes                         |
| 17 | JoinGroup       | V4      | Low         | 🟢 | ⏭️ | ⏭️ | No changes                         |
| 18 | JoinGroup       | V5      | Low         | 🟢 | ⏭️ | ⏭️ | +group_instance_id                 |
| 19 | JoinGroup       | V6      | Medium      | 🟢 | ⏭️ | ⏭️ | FLEX                               |
| 20 | SyncGroup       | V2      | Low         | ⬜ | ⬜ | ⬜ | No changes                         |
| 21 | SyncGroup       | V3      | Low-Med     | ⬜ | ⬜ | ⬜ | +group_instance_id / +protocol_*   |
| 22 | SyncGroup       | V4      | Medium      | ⬜ | ⬜ | ⬜ | FLEX                               |
| 23 | LeaveGroup      | V2      | Low         | ⬜ | ⬜ | ⬜ | No changes                         |
| 24 | LeaveGroup      | V3      | High        | ⬜ | ⬜ | ⬜ | Batch leave (structural change)    |
| 25 | LeaveGroup      | V4      | Medium      | ⬜ | ⬜ | ⬜ | FLEX                               |
| 26 | OffsetFetch     | V4      | Low         | 🟢 | ⏭️ | ⏭️ | No changes                         |
| 27 | OffsetFetch     | V5      | Low         | 🟢 | ⏭️ | ⏭️ | +committed_leader_epoch            |
| 28 | OffsetFetch     | V6      | Medium      | 🟢 | ⏭️ | ⏭️ | FLEX                               |
| 29 | OffsetCommit    | V4      | Low         | 🟢 | ⏭️ | ⏭️ | No changes                         |
| 30 | OffsetCommit    | V5      | Low         | 🟢 | ⏭️ | ⏭️ | -retention_time_ms                 |
| 31 | OffsetCommit    | V6      | Low         | 🟢 | ⏭️ | ⏭️ | +committed_leader_epoch            |
| 32 | OffsetCommit    | V7      | Low         | 🟢 | ⏭️ | ⏭️ | +group_instance_id                 |
| 33 | OffsetCommit    | V8      | Medium      | 🟢 | ⏭️ | ⏭️ | FLEX                               |
| 34 | DescribeGroups  | V2      | Low         | ⬜ | ⬜ | ⬜ | No changes                         |
| 35 | DescribeGroups  | V3      | Low         | ⬜ | ⬜ | ⬜ | +authorized_operations             |
| 36 | DescribeGroups  | V4      | Low         | ⬜ | ⬜ | ⬜ | No changes                         |
| 37 | DescribeGroups  | V5      | Medium      | ⬜ | ⬜ | ⬜ | FLEX                               |
| 38 | CreateTopics    | V3      | Low         | ⬜ | ⬜ | ⬜ | No changes                         |
| 39 | CreateTopics    | V4      | Low         | ⬜ | ⬜ | ⬜ | No changes                         |
| 40 | CreateTopics    | V5      | Medium-High | ⬜ | ⬜ | ⬜ | FLEX + new response fields         |
| 41 | DeleteTopics    | V2      | Low         | ⬜ | ⬜ | ⬜ | No changes                         |
| 42 | DeleteTopics    | V3      | Low         | ⬜ | ⬜ | ⬜ | No changes                         |
| 43 | DeleteTopics    | V4      | Medium      | ⬜ | ⬜ | ⬜ | FLEX                               |
| 44 | ApiVersions     | V2      | Low         | 🟢    | ⏭️ | ⏭️ | No changes                         |
| 45 | ApiVersions     | V3      | Medium      | 🟢    | ⏭️ | ⏭️ | FLEX + client_software fields      |

---

## Summary

- **Total new versions to implement:** 45 (16 remaining)
- **Completed:** 29 versions (ApiVersions V2, V3; Metadata V3-V9; Produce V6, V7, V8; Fetch V8-V11; ListOffsets V3, V4, V5; OffsetFetch V4, V5, V6; OffsetCommit V4, V5, V6, V7, V8; FindCoordinator V2, V3; JoinGroup V3, V4, V5, V6)
- **Low effort:** 9 versions remaining (mostly schema-identical or single field additions)
- **Medium effort:** 5 versions remaining (flexible version encoding changes)
- **High effort:** 1 version (LeaveGroup V3 structural change)
- **Medium-High effort:** 1 version (CreateTopics V5 response additions)
