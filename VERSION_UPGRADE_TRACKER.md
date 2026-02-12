# Kafka API Version Upgrade Tracker

Track implementation of new Kayrock-supported API versions in KafkaEx.

## Legend

| Symbol                | Meaning                                              |
|-----------------------|------------------------------------------------------|
| :white_check_mark:    | Implemented and tested                               |
| :construction:        | In progress                                          |
| :black_square_button: | Not started                                          |
| SKIP                  | Intentionally skipped (no meaningful changes)        |
| FLEX                  | Flexible version (compact encodings + tagged_fields) |

**Test columns:** Unit = protocol-layer unit tests, Integ = integration tests (live broker), Chaos = chaos/fault-injection tests.

---

## 1. ApiVersions (API Key 18)

**Current:** V0-V1 | **Available:** V0-V3

| Version | Status                | Request Changes                                                             | Response Changes                       | Effort | Unit                  | Integ                 | Chaos                 |
|---------|-----------------------|-----------------------------------------------------------------------------|----------------------------------------|--------|-----------------------|-----------------------|-----------------------|
| V0      | :white_check_mark:    | —                                                                           | —                                      | —      | :white_check_mark:    | :white_check_mark:    | :black_square_button: |
| V1      | :white_check_mark:    | —                                                                           | —                                      | —      | :white_check_mark:    | :white_check_mark:    | :black_square_button: |
| V2      | :black_square_button: | No changes vs V1                                                            | No changes vs V1                       | Low    | :black_square_button: | :black_square_button: | :black_square_button: |
| V3      | :black_square_button: | FLEX: +`client_software_name`, +`client_software_version`, +`tagged_fields` | FLEX: +`tagged_fields`, compact arrays | Medium | :black_square_button: | :black_square_button: | :black_square_button: |

---

## 2. Metadata (API Key 3)

**Current:** V0-V2 + `@fallback_to_any` (V3-V9 handled generically) | **Available:** V0-V9

Already covered by `Any` fallback. No action needed.

---

## 3. Produce (API Key 0)

**Current:** V0-V5 | **Available:** V0-V8

| Version | Status                | Request Changes  | Response Changes                                                  | Effort | Unit                  | Integ                 | Chaos                 |
|---------|-----------------------|------------------|-------------------------------------------------------------------|--------|-----------------------|-----------------------|-----------------------|
| V0-V5   | :white_check_mark:    | —                | —                                                                 | —      | :white_check_mark:    | :white_check_mark:    | :white_check_mark:    |
| V6      | :black_square_button: | No changes vs V5 | No changes vs V5                                                  | Low    | :black_square_button: | :black_square_button: | :black_square_button: |
| V7      | :black_square_button: | No changes vs V6 | No changes vs V6                                                  | Low    | :black_square_button: | :black_square_button: | :black_square_button: |
| V8      | :black_square_button: | No changes vs V7 | +`record_errors` array, +`error_message` in partition_responses   | Medium | :black_square_button: | :black_square_button: | :black_square_button: |

---

## 4. Fetch (API Key 1)

**Current:** V0-V7 | **Available:** V0-V11

| Version | Status                | Request Changes                          | Response Changes   | Effort | Unit                  | Integ                 | Chaos                 |
|---------|-----------------------|------------------------------------------|--------------------|--------|-----------------------|-----------------------|-----------------------|
| V0-V7   | :white_check_mark:    | —                                        | —                  | —      | :white_check_mark:    | :white_check_mark:    | :white_check_mark:    |
| V8      | :black_square_button: | No changes vs V7                         | No changes vs V7   | Low    | :black_square_button: | :black_square_button: | :black_square_button: |
| V9      | :black_square_button: | +`current_leader_epoch` in partitions    | No changes vs V8   | Low    | :black_square_button: | :black_square_button: | :black_square_button: |
| V10     | :black_square_button: | No changes vs V9                         | No changes vs V9   | Low    | :black_square_button: | :black_square_button: | :black_square_button: |
| V11     | :black_square_button: | +`rack_id` (top-level)                   | No changes vs V10  | Low    | :black_square_button: | :black_square_button: | :black_square_button: |

---

## 5. ListOffsets (API Key 2)

**Current:** V0-V2 | **Available:** V0-V5

| Version | Status                | Request Changes                          | Response Changes                  | Effort | Unit                  | Integ                 | Chaos                 |
|---------|-----------------------|------------------------------------------|-----------------------------------|--------|-----------------------|-----------------------|-----------------------|
| V0-V2   | :white_check_mark:    | —                                        | —                                 | —      | :white_check_mark:    | :white_check_mark:    | :black_square_button: |
| V3      | :black_square_button: | +`current_leader_epoch` in partitions    | No changes vs V2                  | Low    | :black_square_button: | :black_square_button: | :black_square_button: |
| V4      | :black_square_button: | No changes vs V3                         | +`leader_epoch` in partitions     | Low    | :black_square_button: | :black_square_button: | :black_square_button: |
| V5      | :black_square_button: | No changes vs V4                         | No changes vs V4                  | Low    | :black_square_button: | :black_square_button: | :black_square_button: |

---

## 6. OffsetFetch (API Key 9)

**Current:** V0-V3 | **Available:** V0-V6

| Version | Status                | Request Changes                          | Response Changes                         | Effort | Unit                  | Integ                 | Chaos                 |
|---------|-----------------------|------------------------------------------|------------------------------------------|--------|-----------------------|-----------------------|-----------------------|
| V0-V3   | :white_check_mark:    | —                                        | —                                        | —      | :white_check_mark:    | :white_check_mark:    | :black_square_button: |
| V4      | :black_square_button: | No changes vs V3                         | No changes vs V3                         | Low    | :black_square_button: | :black_square_button: | :black_square_button: |
| V5      | :black_square_button: | No changes vs V4                         | No changes vs V4                         | Low    | :black_square_button: | :black_square_button: | :black_square_button: |
| V6      | :black_square_button: | FLEX: +`tagged_fields`, compact types    | FLEX: +`tagged_fields`, compact types    | Medium | :black_square_button: | :black_square_button: | :black_square_button: |

---

## 7. OffsetCommit (API Key 8)

**Current:** V0-V3 | **Available:** V0-V8

| Version | Status                | Request Changes                             | Response Changes                         | Effort | Unit                  | Integ                 | Chaos                 |
|---------|-----------------------|---------------------------------------------|------------------------------------------|--------|-----------------------|-----------------------|-----------------------|
| V0-V3   | :white_check_mark:    | —                                           | —                                        | —      | :white_check_mark:    | :white_check_mark:    | :black_square_button: |
| V4      | :black_square_button: | No changes vs V3                            | No changes vs V3                         | Low    | :black_square_button: | :black_square_button: | :black_square_button: |
| V5      | :black_square_button: | -`retention_time_ms` removed                | No changes vs V4                         | Low    | :black_square_button: | :black_square_button: | :black_square_button: |
| V6      | :black_square_button: | +`committed_leader_epoch` in partitions     | No changes vs V5                         | Low    | :black_square_button: | :black_square_button: | :black_square_button: |
| V7      | :black_square_button: | +`group_instance_id`                        | No changes vs V6                         | Low    | :black_square_button: | :black_square_button: | :black_square_button: |
| V8      | :black_square_button: | FLEX: +`tagged_fields`, compact types       | FLEX: +`tagged_fields`, compact types    | Medium | :black_square_button: | :black_square_button: | :black_square_button: |

---

## 8. FindCoordinator (API Key 10)

**Current:** V0-V1 | **Available:** V0-V3

| Version | Status                | Request Changes                             | Response Changes                            | Effort | Unit                  | Integ                 | Chaos                 |
|---------|-----------------------|---------------------------------------------|---------------------------------------------|--------|-----------------------|-----------------------|-----------------------|
| V0-V1   | :white_check_mark:    | —                                           | —                                           | —      | :white_check_mark:    | :white_check_mark:    | :black_square_button: |
| V2      | :black_square_button: | No changes vs V1                            | No changes vs V1                            | Low    | :black_square_button: | :black_square_button: | :black_square_button: |
| V3      | :black_square_button: | FLEX: +`tagged_fields`, compact strings     | FLEX: +`tagged_fields`, compact strings     | Medium | :black_square_button: | :black_square_button: | :black_square_button: |

---

## 9. JoinGroup (API Key 11)

**Current:** V0-V2 | **Available:** V0-V6

| Version | Status                | Request Changes                          | Response Changes                            | Effort | Unit                  | Integ                 | Chaos                 |
|---------|-----------------------|------------------------------------------|---------------------------------------------|--------|-----------------------|-----------------------|-----------------------|
| V0-V2   | :white_check_mark:    | —                                        | —                                           | —      | :white_check_mark:    | :white_check_mark:    | :black_square_button: |
| V3      | :black_square_button: | No changes vs V2                         | No changes vs V2                            | Low    | :black_square_button: | :black_square_button: | :black_square_button: |
| V4      | :black_square_button: | No changes vs V3                         | No changes vs V3                            | Low    | :black_square_button: | :black_square_button: | :black_square_button: |
| V5      | :black_square_button: | +`group_instance_id`                     | +`group_instance_id` in members             | Low    | :black_square_button: | :black_square_button: | :black_square_button: |
| V6      | :black_square_button: | FLEX: +`tagged_fields`, compact types    | FLEX: +`tagged_fields`, compact types       | Medium | :black_square_button: | :black_square_button: | :black_square_button: |

---

## 10. SyncGroup (API Key 14)

**Current:** V0-V1 | **Available:** V0-V4

| Version | Status                | Request Changes                          | Response Changes                            | Effort  | Unit                  | Integ                 | Chaos                 |
|---------|-----------------------|------------------------------------------|---------------------------------------------|---------|---------------------- |-----------------------|-----------------------|
| V0-V1   | :white_check_mark:    | —                                        | —                                           | —       | :white_check_mark:    | :white_check_mark:    | :black_square_button: |
| V2      | :black_square_button: | No changes vs V1                         | No changes vs V1                            | Low     | :black_square_button: | :black_square_button: | :black_square_button: |
| V3      | :black_square_button: | +`group_instance_id`                     | +`protocol_type`, +`protocol_name`          | Low-Med | :black_square_button: | :black_square_button: | :black_square_button: |
| V4      | :black_square_button: | FLEX: +`tagged_fields`, compact types    | FLEX: +`tagged_fields`, compact types       | Medium  | :black_square_button: | :black_square_button: | :black_square_button: |

---

## 11. Heartbeat (API Key 12)

**Current:** V0-V1 | **Available:** V0-V4

| Version | Status                | Request Changes                          | Response Changes                         | Effort | Unit                  | Integ                 | Chaos                 |
|---------|-----------------------|------------------------------------------|------------------------------------------|--------|-----------------------|-----------------------|-----------------------|
| V0-V1   | :white_check_mark:    | —                                        | —                                        | —      | :white_check_mark:    | :white_check_mark:    | :black_square_button: |
| V2      | :black_square_button: | No changes vs V1                         | No changes vs V1                         | Low    | :black_square_button: | :black_square_button: | :black_square_button: |
| V3      | :black_square_button: | +`group_instance_id`                     | No changes vs V2                         | Low    | :black_square_button: | :black_square_button: | :black_square_button: |
| V4      | :black_square_button: | FLEX: +`tagged_fields`, compact types    | FLEX: +`tagged_fields`, compact types    | Medium | :black_square_button: | :black_square_button: | :black_square_button: |

---

## 12. LeaveGroup (API Key 13)

**Current:** V0-V1 | **Available:** V0-V4

| Version | Status                | Request Changes                                                         | Response Changes                          | Effort | Unit                  | Integ                 | Chaos                 |
|---------|-----------------------|-------------------------------------------------------------------------|-------------------------------------------|--------|-----------------------|-----------------------|-----------------------|
| V0-V1   | :white_check_mark:    | —                                                                       | —                                         | —      | :white_check_mark:    | :white_check_mark:    | :black_square_button: |
| V2      | :black_square_button: | No changes vs V1                                                        | No changes vs V1                          | Low    | :black_square_button: | :black_square_button: | :black_square_button: |
| V3      | :black_square_button: | **BREAKING:** -`member_id` -> +`members` array (batch leave, KIP-345)  | +`members` array with per-member errors   | High   | :black_square_button: | :black_square_button: | :black_square_button: |
| V4      | :black_square_button: | FLEX: +`tagged_fields`, compact types                                   | FLEX: +`tagged_fields`, compact types     | Medium | :black_square_button: | :black_square_button: | :black_square_button: |

---

## 13. DescribeGroups (API Key 15)

**Current:** V0-V1 | **Available:** V0-V5

| Version | Status                | Request Changes                          | Response Changes                          | Effort | Unit                  | Integ                 | Chaos                 |
|---------|-----------------------|------------------------------------------|-------------------------------------------|--------|-----------------------|-----------------------|-----------------------|
| V0-V1   | :white_check_mark:    | —                                        | —                                         | —      | :white_check_mark:    | :white_check_mark:    | :black_square_button: |
| V2      | :black_square_button: | No changes vs V1                         | No changes vs V1                          | Low    | :black_square_button: | :black_square_button: | :black_square_button: |
| V3      | :black_square_button: | +`include_authorized_operations`         | +`authorized_operations` in groups        | Low    | :black_square_button: | :black_square_button: | :black_square_button: |
| V4      | :black_square_button: | No changes vs V3                         | No changes vs V3                          | Low    | :black_square_button: | :black_square_button: | :black_square_button: |
| V5      | :black_square_button: | FLEX: +`tagged_fields`, compact types    | FLEX: +`tagged_fields`, compact types     | Medium | :black_square_button: | :black_square_button: | :black_square_button: |

---

## 14. CreateTopics (API Key 19)

**Current:** V0-V2 | **Available:** V0-V5

| Version | Status                | Request Changes                          | Response Changes                                                                       | Effort      | Unit                  | Integ                 | Chaos                 |
|---------|-----------------------|------------------------------------------|----------------------------------------------------------------------------------------|-------------|-----------------------|-----------------------|-----------------------|
| V0-V2   | :white_check_mark:    | —                                        | —                                                                                      | —           | :white_check_mark:    | :white_check_mark:    | :black_square_button: |
| V3      | :black_square_button: | No changes vs V2                         | No changes vs V2                                                                       | Low         | :black_square_button: | :black_square_button: | :black_square_button: |
| V4      | :black_square_button: | No changes vs V3                         | No changes vs V3                                                                       | Low         | :black_square_button: | :black_square_button: | :black_square_button: |
| V5      | :black_square_button: | FLEX: +`tagged_fields`, compact types    | FLEX: +`num_partitions`, +`replication_factor`, +`configs` array, +`tagged_fields`     | Medium-High | :black_square_button: | :black_square_button: | :black_square_button: |

---

## 15. DeleteTopics (API Key 20)

**Current:** V0-V1 | **Available:** V0-V4

| Version | Status                | Request Changes                          | Response Changes                         | Effort | Unit                  | Integ                 | Chaos                 |
|---------|-----------------------|------------------------------------------|------------------------------------------|--------|-----------------------|-----------------------|-----------------------|
| V0-V1   | :white_check_mark:    | —                                        | —                                        | —      | :white_check_mark:    | :white_check_mark:    | :black_square_button: |
| V2      | :black_square_button: | No changes vs V1                         | No changes vs V1                         | Low    | :black_square_button: | :black_square_button: | :black_square_button: |
| V3      | :black_square_button: | No changes vs V2                         | No changes vs V2                         | Low    | :black_square_button: | :black_square_button: | :black_square_button: |
| V4      | :black_square_button: | FLEX: +`tagged_fields`, compact types    | FLEX: +`tagged_fields`, compact types    | Medium | :black_square_button: | :black_square_button: | :black_square_button: |

---

## Implementation Order

Prioritized by: (1) most commonly used APIs first, (2) low-effort versions first within each API, (3) group related versions together.

| #  | API             | Version | Effort      | Unit                  | Integ                 | Chaos                 | Notes                              |
|----|-----------------|---------|-------------|-----------------------|-----------------------|-----------------------|------------------------------------|
| 1  | Fetch           | V8      | Low         | :black_square_button: | :black_square_button: | :black_square_button: | No changes, just wire through      |
| 2  | Fetch           | V9      | Low         | :black_square_button: | :black_square_button: | :black_square_button: | +current_leader_epoch              |
| 3  | Fetch           | V10     | Low         | :black_square_button: | :black_square_button: | :black_square_button: | No changes                         |
| 4  | Fetch           | V11     | Low         | :black_square_button: | :black_square_button: | :black_square_button: | +rack_id                           |
| 5  | Produce         | V6      | Low         | :black_square_button: | :black_square_button: | :black_square_button: | No changes                         |
| 6  | Produce         | V7      | Low         | :black_square_button: | :black_square_button: | :black_square_button: | No changes                         |
| 7  | Produce         | V8      | Medium      | :black_square_button: | :black_square_button: | :black_square_button: | +record_errors in response         |
| 8  | ListOffsets     | V3      | Low         | :black_square_button: | :black_square_button: | :black_square_button: | +current_leader_epoch              |
| 9  | ListOffsets     | V4      | Low         | :black_square_button: | :black_square_button: | :black_square_button: | +leader_epoch in response          |
| 10 | ListOffsets     | V5      | Low         | :black_square_button: | :black_square_button: | :black_square_button: | No changes                         |
| 11 | FindCoordinator | V2      | Low         | :black_square_button: | :black_square_button: | :black_square_button: | No changes                         |
| 12 | FindCoordinator | V3      | Medium      | :black_square_button: | :black_square_button: | :black_square_button: | FLEX                               |
| 13 | Heartbeat       | V2      | Low         | :black_square_button: | :black_square_button: | :black_square_button: | No changes                         |
| 14 | Heartbeat       | V3      | Low         | :black_square_button: | :black_square_button: | :black_square_button: | +group_instance_id                 |
| 15 | Heartbeat       | V4      | Medium      | :black_square_button: | :black_square_button: | :black_square_button: | FLEX                               |
| 16 | JoinGroup       | V3      | Low         | :black_square_button: | :black_square_button: | :black_square_button: | No changes                         |
| 17 | JoinGroup       | V4      | Low         | :black_square_button: | :black_square_button: | :black_square_button: | No changes                         |
| 18 | JoinGroup       | V5      | Low         | :black_square_button: | :black_square_button: | :black_square_button: | +group_instance_id                 |
| 19 | JoinGroup       | V6      | Medium      | :black_square_button: | :black_square_button: | :black_square_button: | FLEX                               |
| 20 | SyncGroup       | V2      | Low         | :black_square_button: | :black_square_button: | :black_square_button: | No changes                         |
| 21 | SyncGroup       | V3      | Low-Med     | :black_square_button: | :black_square_button: | :black_square_button: | +group_instance_id / +protocol_*   |
| 22 | SyncGroup       | V4      | Medium      | :black_square_button: | :black_square_button: | :black_square_button: | FLEX                               |
| 23 | LeaveGroup      | V2      | Low         | :black_square_button: | :black_square_button: | :black_square_button: | No changes                         |
| 24 | LeaveGroup      | V3      | High        | :black_square_button: | :black_square_button: | :black_square_button: | Batch leave (structural change)    |
| 25 | LeaveGroup      | V4      | Medium      | :black_square_button: | :black_square_button: | :black_square_button: | FLEX                               |
| 26 | OffsetFetch     | V4      | Low         | :black_square_button: | :black_square_button: | :black_square_button: | No changes                         |
| 27 | OffsetFetch     | V5      | Low         | :black_square_button: | :black_square_button: | :black_square_button: | No changes                         |
| 28 | OffsetFetch     | V6      | Medium      | :black_square_button: | :black_square_button: | :black_square_button: | FLEX                               |
| 29 | OffsetCommit    | V4      | Low         | :black_square_button: | :black_square_button: | :black_square_button: | No changes                         |
| 30 | OffsetCommit    | V5      | Low         | :black_square_button: | :black_square_button: | :black_square_button: | -retention_time_ms                 |
| 31 | OffsetCommit    | V6      | Low         | :black_square_button: | :black_square_button: | :black_square_button: | +committed_leader_epoch            |
| 32 | OffsetCommit    | V7      | Low         | :black_square_button: | :black_square_button: | :black_square_button: | +group_instance_id                 |
| 33 | OffsetCommit    | V8      | Medium      | :black_square_button: | :black_square_button: | :black_square_button: | FLEX                               |
| 34 | DescribeGroups  | V2      | Low         | :black_square_button: | :black_square_button: | :black_square_button: | No changes                         |
| 35 | DescribeGroups  | V3      | Low         | :black_square_button: | :black_square_button: | :black_square_button: | +authorized_operations             |
| 36 | DescribeGroups  | V4      | Low         | :black_square_button: | :black_square_button: | :black_square_button: | No changes                         |
| 37 | DescribeGroups  | V5      | Medium      | :black_square_button: | :black_square_button: | :black_square_button: | FLEX                               |
| 38 | CreateTopics    | V3      | Low         | :black_square_button: | :black_square_button: | :black_square_button: | No changes                         |
| 39 | CreateTopics    | V4      | Low         | :black_square_button: | :black_square_button: | :black_square_button: | No changes                         |
| 40 | CreateTopics    | V5      | Medium-High | :black_square_button: | :black_square_button: | :black_square_button: | FLEX + new response fields         |
| 41 | DeleteTopics    | V2      | Low         | :black_square_button: | :black_square_button: | :black_square_button: | No changes                         |
| 42 | DeleteTopics    | V3      | Low         | :black_square_button: | :black_square_button: | :black_square_button: | No changes                         |
| 43 | DeleteTopics    | V4      | Medium      | :black_square_button: | :black_square_button: | :black_square_button: | FLEX                               |
| 44 | ApiVersions     | V2      | Low         | :black_square_button: | :black_square_button: | :black_square_button: | No changes                         |
| 45 | ApiVersions     | V3      | Medium      | :black_square_button: | :black_square_button: | :black_square_button: | FLEX + client_software fields      |

---

## Summary

- **Total new versions to implement:** 45
- **Low effort:** 30 versions (mostly schema-identical or single field additions)
- **Medium effort:** 13 versions (flexible version encoding changes)
- **High effort:** 1 version (LeaveGroup V3 structural change)
- **Medium-High effort:** 1 version (CreateTopics V5 response additions)
