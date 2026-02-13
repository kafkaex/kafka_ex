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
| V8      | 🟢     | No changes vs V7                         | No changes vs V7                   | Low    | 🟢   | 🟢    | ⬜    |
| V9      | 🟢     | +`current_leader_epoch` in partitions    | No changes vs V8                   | Low    | 🟢   | 🟢    | ⬜    |
| V10     | 🟢     | No changes vs V9                         | No changes vs V9                   | Low    | 🟢   | 🟢    | ⬜    |
| V11     | 🟢     | +`rack_id` (top-level)                   | +`preferred_read_replica` per part | Low    | 🟢   | 🟢    | ⬜    |

---

## 5. ListOffsets (API Key 2)

**Current:** V0-V2 | **Available:** V0-V5

| Version | Status                | Request Changes                          | Response Changes                  | Effort | Unit                  | Integ                 | Chaos                 |
|---------|-----------------------|------------------------------------------|-----------------------------------|--------|-----------------------|-----------------------|-----------------------|
| V0-V2   | 🟢    | —                                        | —                                 | —      | 🟢    | 🟢    | ⬜ |
| V3      | ⬜ | +`current_leader_epoch` in partitions    | No changes vs V2                  | Low    | ⬜ | ⬜ | ⬜ |
| V4      | ⬜ | No changes vs V3                         | +`leader_epoch` in partitions     | Low    | ⬜ | ⬜ | ⬜ |
| V5      | ⬜ | No changes vs V4                         | No changes vs V4                  | Low    | ⬜ | ⬜ | ⬜ |

---

## 6. OffsetFetch (API Key 9)

**Current:** V0-V3 | **Available:** V0-V6

| Version | Status                | Request Changes                          | Response Changes                         | Effort | Unit                  | Integ                 | Chaos                 |
|---------|-----------------------|------------------------------------------|------------------------------------------|--------|-----------------------|-----------------------|-----------------------|
| V0-V3   | 🟢    | —                                        | —                                        | —      | 🟢    | 🟢    | ⬜ |
| V4      | ⬜ | No changes vs V3                         | No changes vs V3                         | Low    | ⬜ | ⬜ | ⬜ |
| V5      | ⬜ | No changes vs V4                         | No changes vs V4                         | Low    | ⬜ | ⬜ | ⬜ |
| V6      | ⬜ | FLEX: +`tagged_fields`, compact types    | FLEX: +`tagged_fields`, compact types    | Medium | ⬜ | ⬜ | ⬜ |

---

## 7. OffsetCommit (API Key 8)

**Current:** V0-V3 | **Available:** V0-V8

| Version | Status                | Request Changes                             | Response Changes                         | Effort | Unit                  | Integ                 | Chaos                 |
|---------|-----------------------|---------------------------------------------|------------------------------------------|--------|-----------------------|-----------------------|-----------------------|
| V0-V3   | 🟢    | —                                           | —                                        | —      | 🟢    | 🟢    | ⬜ |
| V4      | ⬜ | No changes vs V3                            | No changes vs V3                         | Low    | ⬜ | ⬜ | ⬜ |
| V5      | ⬜ | -`retention_time_ms` removed                | No changes vs V4                         | Low    | ⬜ | ⬜ | ⬜ |
| V6      | ⬜ | +`committed_leader_epoch` in partitions     | No changes vs V5                         | Low    | ⬜ | ⬜ | ⬜ |
| V7      | ⬜ | +`group_instance_id`                        | No changes vs V6                         | Low    | ⬜ | ⬜ | ⬜ |
| V8      | ⬜ | FLEX: +`tagged_fields`, compact types       | FLEX: +`tagged_fields`, compact types    | Medium | ⬜ | ⬜ | ⬜ |

---

## 8. FindCoordinator (API Key 10)

**Current:** V0-V1 | **Available:** V0-V3

| Version | Status                | Request Changes                             | Response Changes                            | Effort | Unit                  | Integ                 | Chaos                 |
|---------|-----------------------|---------------------------------------------|---------------------------------------------|--------|-----------------------|-----------------------|-----------------------|
| V0-V1   | 🟢    | —                                           | —                                           | —      | 🟢    | 🟢    | ⬜ |
| V2      | ⬜ | No changes vs V1                            | No changes vs V1                            | Low    | ⬜ | ⬜ | ⬜ |
| V3      | ⬜ | FLEX: +`tagged_fields`, compact strings     | FLEX: +`tagged_fields`, compact strings     | Medium | ⬜ | ⬜ | ⬜ |

---

## 9. JoinGroup (API Key 11)

**Current:** V0-V2 | **Available:** V0-V6

| Version | Status                | Request Changes                          | Response Changes                            | Effort | Unit                  | Integ                 | Chaos                 |
|---------|-----------------------|------------------------------------------|---------------------------------------------|--------|-----------------------|-----------------------|-----------------------|
| V0-V2   | 🟢    | —                                        | —                                           | —      | 🟢    | 🟢    | ⬜ |
| V3      | ⬜ | No changes vs V2                         | No changes vs V2                            | Low    | ⬜ | ⬜ | ⬜ |
| V4      | ⬜ | No changes vs V3                         | No changes vs V3                            | Low    | ⬜ | ⬜ | ⬜ |
| V5      | ⬜ | +`group_instance_id`                     | +`group_instance_id` in members             | Low    | ⬜ | ⬜ | ⬜ |
| V6      | ⬜ | FLEX: +`tagged_fields`, compact types    | FLEX: +`tagged_fields`, compact types       | Medium | ⬜ | ⬜ | ⬜ |

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
| 1  | Fetch           | V8      | Low         | 🟢 | 🟢 | ⬜ | No changes, just wire through      |
| 2  | Fetch           | V9      | Low         | 🟢 | 🟢 | ⬜ | +current_leader_epoch              |
| 3  | Fetch           | V10     | Low         | 🟢 | 🟢 | ⬜ | No changes                         |
| 4  | Fetch           | V11     | Low         | 🟢 | 🟢 | ⬜ | +rack_id                           |
| 5  | Produce         | V6      | Low         | 🟢 | ⏭️ | ⏭️ | No changes                         |
| 6  | Produce         | V7      | Low         | 🟢 | ⏭️ | ⏭️ | No changes                         |
| 7  | Produce         | V8      | Medium      | 🟢 | ⏭️ | ⏭️ | +record_errors in response         |
| 8  | ListOffsets     | V3      | Low         | ⬜ | ⬜ | ⬜ | +current_leader_epoch              |
| 9  | ListOffsets     | V4      | Low         | ⬜ | ⬜ | ⬜ | +leader_epoch in response          |
| 10 | ListOffsets     | V5      | Low         | ⬜ | ⬜ | ⬜ | No changes                         |
| 11 | FindCoordinator | V2      | Low         | ⬜ | ⬜ | ⬜ | No changes                         |
| 12 | FindCoordinator | V3      | Medium      | ⬜ | ⬜ | ⬜ | FLEX                               |
| 13 | Heartbeat       | V2      | Low         | ⬜ | ⬜ | ⬜ | No changes                         |
| 14 | Heartbeat       | V3      | Low         | ⬜ | ⬜ | ⬜ | +group_instance_id                 |
| 15 | Heartbeat       | V4      | Medium      | ⬜ | ⬜ | ⬜ | FLEX                               |
| 16 | JoinGroup       | V3      | Low         | ⬜ | ⬜ | ⬜ | No changes                         |
| 17 | JoinGroup       | V4      | Low         | ⬜ | ⬜ | ⬜ | No changes                         |
| 18 | JoinGroup       | V5      | Low         | ⬜ | ⬜ | ⬜ | +group_instance_id                 |
| 19 | JoinGroup       | V6      | Medium      | ⬜ | ⬜ | ⬜ | FLEX                               |
| 20 | SyncGroup       | V2      | Low         | ⬜ | ⬜ | ⬜ | No changes                         |
| 21 | SyncGroup       | V3      | Low-Med     | ⬜ | ⬜ | ⬜ | +group_instance_id / +protocol_*   |
| 22 | SyncGroup       | V4      | Medium      | ⬜ | ⬜ | ⬜ | FLEX                               |
| 23 | LeaveGroup      | V2      | Low         | ⬜ | ⬜ | ⬜ | No changes                         |
| 24 | LeaveGroup      | V3      | High        | ⬜ | ⬜ | ⬜ | Batch leave (structural change)    |
| 25 | LeaveGroup      | V4      | Medium      | ⬜ | ⬜ | ⬜ | FLEX                               |
| 26 | OffsetFetch     | V4      | Low         | ⬜ | ⬜ | ⬜ | No changes                         |
| 27 | OffsetFetch     | V5      | Low         | ⬜ | ⬜ | ⬜ | No changes                         |
| 28 | OffsetFetch     | V6      | Medium      | ⬜ | ⬜ | ⬜ | FLEX                               |
| 29 | OffsetCommit    | V4      | Low         | ⬜ | ⬜ | ⬜ | No changes                         |
| 30 | OffsetCommit    | V5      | Low         | ⬜ | ⬜ | ⬜ | -retention_time_ms                 |
| 31 | OffsetCommit    | V6      | Low         | ⬜ | ⬜ | ⬜ | +committed_leader_epoch            |
| 32 | OffsetCommit    | V7      | Low         | ⬜ | ⬜ | ⬜ | +group_instance_id                 |
| 33 | OffsetCommit    | V8      | Medium      | ⬜ | ⬜ | ⬜ | FLEX                               |
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

- **Total new versions to implement:** 45 (33 remaining)
- **Completed:** 12 versions (ApiVersions V2, V3; Metadata V3-V9; Produce V6, V7, V8)
- **Low effort:** 22 versions remaining (mostly schema-identical or single field additions)
- **Medium effort:** 9 versions remaining (flexible version encoding changes)
- **High effort:** 1 version (LeaveGroup V3 structural change)
- **Medium-High effort:** 1 version (CreateTopics V5 response additions)
