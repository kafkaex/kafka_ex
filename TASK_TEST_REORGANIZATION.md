# Task: Integration Test Reorganization

## Current State Analysis

### Tags in `test/test_helper.exs` (Excluded by Default)

| Tag | Status | Description |
|-----|--------|-------------|
| `new_client: true` | **VALID** | Tests specific to the new Kayrock-based client |
| `integration: true` | **VALID** | General integration tests requiring Kafka cluster |
| `consumer_group: true` | **VALID** | Consumer group coordination tests |
| `server_0_p_10_p_1: true` | **OBSOLETE** | Legacy server v0.10.1 tests - servers removed |
| `server_0_p_10_and_later: true` | **OBSOLETE** | Legacy server v0.10+ tests - servers removed |
| `server_0_p_9_p_0: true` | **OBSOLETE** | Legacy server v0.9.0 tests - servers removed |
| `server_0_p_8_p_0: true` | **OBSOLETE** | Legacy server v0.8.0 tests - servers removed |
| `sasl: true` | **VALID** | SASL authentication tests |

### Current CI Workflow Structure (`integration.yml`)

| Job | Script | Status |
|-----|--------|--------|
| `legacy-tests` | `ci_tests_legacy.sh` | **OBSOLETE** - Legacy servers removed |
| `integration-tests` | `ci_tests_integration.sh` | **VALID** |
| `auth-tests` | `ci_tests_auth.sh` | **VALID** |

### Current Integration Test Files

```
test/integration/
├── kafka_ex_api_test.exs        # @moduletag :integration - API tests
├── fetch_kayrock_test.exs       # @moduletag :integration - Fetch tests
├── produce_kayrock_test.exs     # @moduletag :integration - Produce tests
├── new_client_test.exs          # @moduletag :new_client - Client tests
├── sasl_authentication_test.exs # @moduletag :integration, @tag sasl: :plain/:scram
└── oauthbearer_authentication_test.exs # @moduletag :integration, :oauthbearer
```

---

## Proposed New Integration Test Structure

### New Tag Categories

| Tag | Description |
|-----|-------------|
| `api` | Core API operations (produce, fetch, offsets, metadata, topic management) |
| `consumer_group` | Consumer group coordination (join, sync, heartbeat, leave, describe) |
| `full_cycle` | End-to-end workflows (produce→fetch round trips, streaming, consumers) |
| `authorization` | Authentication tests (SASL PLAIN, SCRAM-SHA-256/512, OAUTHBEARER) |
| `edge_cases` | Boundary conditions, unusual inputs, partition limits |
| `error_handling` | Error scenarios, recovery, reconnection, timeouts |

---

## Test Suite Definitions

### 1. API Tests (`@tag :api`)

**Purpose:** Test core Kafka API operations in isolation.

**What to test:**
- `produce/4,5` - Message production with various options
- `fetch/5` - Message fetching with API versions 0-5
- `latest_offset/3,4` - Getting latest partition offsets
- `earliest_offset/3,4` - Getting earliest partition offsets
- `list_offsets/2` - Batch offset queries
- `metadata/1,2` - Topic and cluster metadata
- `api_versions/1,2` - Broker API version discovery
- `create_topics/2` - Topic creation
- `delete_topics/2` - Topic deletion

**Files:**
```
test/integration/api/
├── produce_test.exs
├── fetch_test.exs
├── offsets_test.exs
├── metadata_test.exs
├── topics_test.exs
└── api_versions_test.exs
```

**Run command:**
```bash
mix test --only api
```

---

### 2. Consumer Group Tests (`@tag :consumer_group`)

**Purpose:** Test consumer group coordination protocol.

**What to test:**
- `join_group/5` - Joining consumer groups
- `sync_group/6` - Synchronizing partition assignments
- `heartbeat/4` - Keeping group membership alive
- `leave_group/4` - Gracefully leaving groups
- `describe_group/2,3` - Inspecting group state
- `find_coordinator/2` - Finding group coordinator
- `fetch_committed_offset/5` - Getting committed offsets
- `commit_offset/5` - Committing offsets
- Partition assignment callbacks
- Rebalancing scenarios

**Files:**
```
test/integration/consumer_group/
├── join_test.exs
├── sync_test.exs
├── heartbeat_test.exs
├── leave_test.exs
├── describe_test.exs
├── offsets_test.exs
├── rebalance_test.exs
└── partition_assignment_test.exs
```

**Run command:**
```bash
mix test --only consumer_group
```

---

### 3. Full Cycle Tests (`@tag :full_cycle`)

**Purpose:** Test end-to-end workflows that exercise the complete system.

**What to test:**
- Produce → Fetch round trips
- Produce with compression → Fetch decompression
- Produce with headers → Fetch with headers
- Produce with keys → Fetch verifying keys
- Streaming consumption (`KafkaEx.Stream`)
- `GenConsumer` lifecycle
- `ConsumerGroup` supervisor
- Multi-partition production/consumption
- Large message handling
- Batch message production/consumption

**Files:**
```
test/integration/full_cycle/
├── produce_fetch_test.exs
├── compression_test.exs
├── headers_test.exs
├── stream_test.exs
├── gen_consumer_test.exs
├── consumer_group_supervisor_test.exs
├── multi_partition_test.exs
└── batch_test.exs
```

**Run command:**
```bash
mix test --only full_cycle
```

---

### 4. Authorization Tests (`@tag :authorization`)

**Purpose:** Test authentication mechanisms with Kafka brokers.

**What to test:**
- SASL/PLAIN authentication
- SASL/SCRAM-SHA-256 authentication
- SASL/SCRAM-SHA-512 authentication
- OAUTHBEARER authentication
- SSL/TLS connections
- Connection with invalid credentials (should fail)
- Token refresh for OAUTHBEARER

**Files:**
```
test/integration/authorization/
├── sasl_plain_test.exs
├── sasl_scram_test.exs
├── oauthbearer_test.exs
├── ssl_test.exs
└── invalid_credentials_test.exs
```

**Run command:**
```bash
mix test --only authorization
```

**Docker ports:**
- 9092-9094: No authentication (SSL)
- 9192-9194: SASL/PLAIN (SSL)
- 9292-9294: SASL/SCRAM (SSL)

---

### 5. Edge Cases Tests (`@tag :edge_cases`)

**Purpose:** Test boundary conditions and unusual inputs.

**What to test:**
- Empty messages (empty value, empty key)
- Binary/null bytes in messages
- Maximum message size
- Unicode/UTF-8 in keys and values
- Very long topic names
- Single partition vs many partitions
- Zero offset handling
- Fetching from future offsets
- Producing to non-existent partitions
- Topic auto-creation behavior

**Files:**
```
test/integration/edge_cases/
├── empty_messages_test.exs
├── binary_data_test.exs
├── large_messages_test.exs
├── unicode_test.exs
├── partition_boundaries_test.exs
└── offset_boundaries_test.exs
```

**Run command:**
```bash
mix test --only edge_cases
```

---

### 6. Error Handling Tests (`@tag :error_handling`)

**Purpose:** Test error scenarios, recovery, and resilience.

**What to test:**
- Connection failures and reconnection
- Broker unavailable scenarios
- Request timeouts
- Invalid API version requests
- Leader not available
- Unknown topic or partition errors
- Offset out of range
- Consumer group coordinator changes
- Metadata refresh on errors
- Socket disconnection handling

**Files:**
```
test/integration/error_handling/
├── connection_test.exs
├── timeout_test.exs
├── broker_errors_test.exs
├── offset_errors_test.exs
├── reconnection_test.exs
└── metadata_refresh_test.exs
```

**Run command:**
```bash
mix test --only error_handling
```

---

## Implementation Plan

### Phase 1: Update test_helper.exs

Remove obsolete tags and add new ones:

```elixir
ExUnit.configure(
  timeout: 120 * 1000,
  exclude: [
    # New categories
    api: true,
    consumer_group: true,
    full_cycle: true,
    authorization: true,
    edge_cases: true,
    error_handling: true,
    # Legacy (keep for backwards compat during transition)
    integration: true,
    new_client: true,
    sasl: true
  ]
)
```

### Phase 2: Update CI Scripts

**`scripts/ci_tests_api.sh`:**
```bash
mix test --only api --only edge_cases || mix test --failed
```

**`scripts/ci_tests_consumer_group.sh`:**
```bash
mix test --only consumer_group || mix test --failed
```

**`scripts/ci_tests_full_cycle.sh`:**
```bash
mix test --only full_cycle || mix test --failed
```

**`scripts/ci_tests_authorization.sh`:**
```bash
mix test --only authorization || mix test --failed
```

**`scripts/ci_tests_error_handling.sh`:**
```bash
mix test --only error_handling || mix test --failed
```

### Phase 3: Update GitHub Workflow

```yaml
# .github/workflows/integration.yml
jobs:
  api-tests:
    name: API Tests
    # ...
    steps:
      - run: ./scripts/ci_tests_api.sh

  consumer-group-tests:
    name: Consumer Group Tests
    # ...
    steps:
      - run: ./scripts/ci_tests_consumer_group.sh

  full-cycle-tests:
    name: Full Cycle Tests
    # ...
    steps:
      - run: ./scripts/ci_tests_full_cycle.sh

  authorization-tests:
    name: Authorization Tests
    # ...
    steps:
      - run: ./scripts/ci_tests_authorization.sh

  error-handling-tests:
    name: Error Handling Tests
    # ...
    steps:
      - run: ./scripts/ci_tests_error_handling.sh
```

### Phase 4: Migrate Existing Tests

| Current File | New Location | New Tag |
|--------------|--------------|---------|
| `kafka_ex_api_test.exs` | `api/core_test.exs` | `:api` |
| `fetch_kayrock_test.exs` | `api/fetch_test.exs` | `:api` |
| `produce_kayrock_test.exs` | `api/produce_test.exs` | `:api` |
| `new_client_test.exs` | `api/client_test.exs` | `:api` |
| `sasl_authentication_test.exs` | `authorization/sasl_test.exs` | `:authorization` |
| `oauthbearer_authentication_test.exs` | `authorization/oauthbearer_test.exs` | `:authorization` |

### Phase 5: Remove Obsolete Items

1. Delete `scripts/ci_tests_legacy.sh`
2. Remove `legacy-tests` job from `integration.yml`
3. Remove obsolete tags from `test_helper.exs`:
   - `server_0_p_10_p_1`
   - `server_0_p_10_and_later`
   - `server_0_p_9_p_0`
   - `server_0_p_8_p_0`

---

## Quick Reference: Running Tests

```bash
# Run all unit tests (no Kafka required)
mix test --no-start

# Run specific integration test suite
mix test --only api
mix test --only consumer_group
mix test --only full_cycle
mix test --only authorization
mix test --only edge_cases
mix test --only error_handling

# Run all integration tests
./scripts/all_tests.sh

# Run with specific seed for reproducibility
mix test --only api --seed 12345
```
