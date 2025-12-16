# Kayrock Migration Status

## Overview

This document tracks the progress of migrating KafkaEx to use Kayrock protocol implementations for all Kafka APIs. 
The migration aims to replace manual binary parsing with Kayrock's type-safe, versioned protocol layer.

**Last Updated:** 2025-12-15
**Overall Progress:** 14/20 APIs migrated (70%)

### Quick Summary

| Category | Status | APIs |
|----------|--------|------|
| ✅ **Fully Migrated** | 14 | Metadata, ApiVersions, Produce, Fetch, ListOffsets, OffsetFetch, OffsetCommit, JoinGroup, SyncGroup, Heartbeat, LeaveGroup, DescribeGroups, FindCoordinator, CreateTopics |
| 🟡 **Adapter Only** | 1 | DeleteTopics |
| ❌ **Not Started** | 5 | CreatePartitions, DeleteRecords, DescribeConfigs, AlterConfigs, InitProducerID |

### Test Status

- **Unit Tests:** 1,809 passing
- **Integration Tests:** 200+ (with Kafka cluster)
- **Code Quality:** mix format ✅, mix credo --strict ✅

---

## System Architecture

### Current Infrastructure

The KafkaEx library implements a dual-layer architecture to support both legacy and new Kayrock-based APIs:

#### 1. **Client Layer** (`lib/kafka_ex/new/client.ex`)
   - GenServer-based client managing connections to Kafka brokers
   - Maintains cluster metadata, API version cache, and broker connections
   - Handles both legacy (2-tuple) and new (4-tuple) request formats
   - Auto-discovers API versions on initialization via `Kayrock.ApiVersions`
   - Pattern: `{:api_name, ...legacy_args...}` → routed to `ClientCompatibility`
   - Pattern: `{:api_name, param1, param2, opts}` → routed to new protocol layer

#### 2. **Protocol Layer** (`lib/kafka_ex/new/protocols/`)
   - **KayrockProtocol** (`kayrock_protocol.ex`): Central dispatcher for request building and response parsing
   - **Individual API Protocols** (`kayrock/{api_name}/`): Per-API versioned implementations
     - Each API has its own module defining Request and Response protocols
     - Version-specific implementations (e.g., `v0_request_impl.ex`, `v1_response_impl.ex`)
     - Shared helpers for common logic across versions (`request_helpers.ex`, `response_helpers.ex`)

#### 3. **Request/Response Infrastructure**
   - **RequestBuilder** (`client/request_builder.ex`):
     - Determines API version based on broker capabilities
     - Builds Kayrock requests from user parameters
     - Default API versions configured per API (mostly v1)
   - **ResponseParser** (`client/response_parser.ex`):
     - Parses Kayrock responses into KafkaEx structs
     - Handles error detection and conversion
     - Returns structured `{:ok, struct}` or `{:error, error}` tuples

#### 4. **Struct Layer** (`lib/kafka_ex/new/structs/`)
   - Type-safe Elixir structs representing Kafka concepts
   - Examples: `ClusterMetadata`, `Broker`, `Topic`, `Partition`, `ConsumerGroup`
   - Protocol-specific structs: `Heartbeat`, `JoinGroup`, `SyncGroup`, `LeaveGroup`, `Offset`
   - Provide helper functions for common operations

#### 5. **Backward Compatibility Layer**
   - **Adapter** (`adapter.ex`): Converts between legacy KafkaEx structs and Kayrock structs
   - **ClientCompatibility** (`client_compatibility.ex`): Injected into Client via `use` macro
     - Implements legacy `handle_call` clauses for 2-tuple requests
     - Translates to new API calls internally
     - Maintains 100% compatibility with existing KafkaEx code

#### 6. **Public API** (`kafka_ex_api.ex`)
   - Modern, ergonomic API for new applications
   - Clean function signatures with keyword options
   - Returns structured results: `{:ok, result}` | `{:error, reason}`
   - Examples: `latest_offset/3`, `describe_group/2`, `heartbeat/4`, `metadata/2`

### State Management

The `Client.State` struct maintains:
- **`cluster_metadata`**: Broker topology, topic/partition mappings, consumer group coordinators
- **`api_versions`**: Cached API version support from brokers (Map of `api_key => {min_version, max_version}`)
- **`correlation_id`**: Request correlation tracking
- **`consumer_group_for_auto_commit`**: Consumer group for legacy auto-commit support
- **Connection details**: SSL options, auth config, bootstrap URIs

### Request Flow (New API)

```
User Code
  ↓
KafkaExAPI.metadata(client, topics, opts)
  ↓
GenServer.call(client, {:metadata, topics, opts, api_version})
  ↓
Client.handle_call({:metadata, topics, opts, _api_version})
  ↓
RequestBuilder.metadata_request(request_opts, state)
  ├─ Determines API version from state.api_versions
  └─ Calls KayrockProtocol.build_request(:metadata, api_version, opts)
      ↓
      Kayrock.Metadata.Request.build_request(struct, opts)
        ├─ Routes to version-specific implementation
        └─ Returns populated Kayrock request struct
  ↓
kayrock_network_request(request, node_selector, state)
  ├─ Serializes request via Kayrock.Request.serialize/1
  ├─ Selects broker via NodeSelector
  ├─ Sends over TCP/SSL socket
  └─ Deserializes response
  ↓
ResponseParser.metadata_response(kayrock_response)
  ├─ Calls KayrockProtocol.parse_response(:metadata, response)
  └─ Converts to ClusterMetadata struct
  ↓
Returns {:ok, ClusterMetadata.t()} to user
```

### API Version Negotiation

On client initialization:
1. Client connects to bootstrap brokers
2. Sends `ApiVersions.V0.Request` to first available broker
3. Broker responds with supported API versions
4. `State.ingest_api_versions/2` stores versions in state
5. `State.max_supported_api_version/3` used by RequestBuilder to select version
   - Takes minimum of: broker's max version, Kayrock's max version, requested version

---

## Migration Strategy

### Architecture Pattern

All migrations follow the established pattern from ListOffsets:

1. **Protocol Layer** - Versioned request/response implementations using Elixir protocols
2. **Infrastructure Integration** - Wire protocols into RequestBuilder/ResponseParser
3. **Client Integration** - Add handle_call clauses with proper routing
4. **Public API** - Modern, clean API in `KafkaEx.New.KafkaExAPI` module
5. **Backward Compatibility** - Maintain legacy API through Adapter layer

### Dual API Design

Each migration maintains two parallel APIs:

- **Legacy API** (2-tuple) → `client_compatibility.ex` → `Adapter` → Kayrock
- **New API** (4-tuple) → `client.ex` → `RequestBuilder` → Protocol layer

Pattern matching naturally separates them with no conflicts.

---

## Migration Status by API

### Core APIs

| API              | Status          | Versions | Priority | Complexity | Notes                           |
|------------------|-----------------|----------|----------|------------|---------------------------------|
| **Metadata**     | ✅ Complete     | v0-v2    | HIGH     | Medium     | Core infrastructure             |
| **ApiVersions**  | ✅ Complete     | v0-v1    | HIGH     | Low        | Version negotiation             |
| **Produce**      | ✅ Complete     | v0-v5    | HIGH     | High       | Message publishing              |
| **Fetch**        | ✅ Complete     | v0-v7    | HIGH     | High       | Message consumption             |
| **ListOffsets**  | ✅ Complete     | v0-v2    | HIGH     | Medium     | Timestamp-based offset lookup   |
| **OffsetFetch**  | ✅ Complete     | v0-v3    | HIGH     | Medium     | Consumer group offset retrieval |
| **OffsetCommit** | ✅ Complete     | v0-v3    | HIGH     | Medium     | Consumer group offset storage   |

### Consumer Group Coordination APIs

| API                | Status          | Versions | Priority | Complexity   | Notes                     |
|--------------------|-----------------|----------|----------|--------------|---------------------------|
| **JoinGroup**      | ✅ Complete     | v0-v2    | HIGH     | High         | Consumer group membership |
| **SyncGroup**      | ✅ Complete     | v0-v1    | HIGH     | Medium       | Partition assignment      |
| **Heartbeat**      | ✅ Complete     | v0-v1    | HIGH     | Low          | Keep-alive mechanism      |
| **LeaveGroup**     | ✅ Complete     | v0-v1    | MEDIUM   | Low          | Clean exit from group     |
| **DescribeGroups** | ✅ Complete     | v0-v1    | HIGH     | Medium       | Group metadata inspection |

### Topic Management APIs

| API                  | Status            | Versions | Priority | Complexity | Notes              |
|----------------------|-------------------|----------|----------|------------|--------------------|
| **CreateTopics**     | ✅ Complete       | v0-v2    | MEDIUM   | Medium     | Topic creation     |
| **DeleteTopics**     | 🟡 Adapter Only   | v0-v3    | MEDIUM   | Low        | Topic deletion     |
| **CreatePartitions** | ❌ Not Started    | v0-v1    | LOW      | Low        | Partition addition |
| **DeleteRecords**    | ❌ Not Started    | v0-v1    | LOW      | Low        | Record deletion    |

### Advanced APIs

| API                    | Status              | Versions | Priority | Complexity | Notes                    |
|------------------------|---------------------|----------|----------|------------|--------------------------|
| **DescribeConfigs**    | ❌ Not Started      | v0-v2    | LOW      | Medium     | Configuration inspection |
| **AlterConfigs**       | ❌ Not Started      | v0-v1    | LOW      | Medium     | Configuration changes    |
| **FindCoordinator**    | ✅ Complete         | v0-v1    | MEDIUM   | Low        | Coordinator discovery    |
| **InitProducerID**     | ❌ Not Started      | v0-v2    | LOW      | Medium     | Transactional support    |
| **AddPartitionsToTxn** | ❌ Not Started      | v0-v1    | LOW      | Medium     | Transactional support    |

## Migration Checklist Template

Use this checklist when migrating any API:

### Phase 1: Data Structures
- [ ] Extend or create structs in `lib/kafka_ex/new/structs/`
- [ ] Add pattern guards if needed for disambiguation
- [ ] Write struct unit tests

### Phase 2: Protocol Implementation
- [ ] Create protocol directory `lib/kafka_ex/new/protocols/kayrock/{api_name}/`
- [ ] Implement protocol definitions (request + response)
- [ ] Create shared helpers module
- [ ] Implement all version request handlers (v0, v1, v2, ...)
- [ ] Implement all version response handlers (v0, v1, v2, ...)
- [ ] Write protocol unit tests (aim for 30-40 tests)

### Phase 3: Infrastructure Integration
- [ ] Update `kayrock_protocol.ex` with build_request/parse_response
- [ ] Update `request_builder.ex` with API-specific builder
- [ ] Update `response_parser.ex` with API-specific parser
- [ ] Update `client.ex` with handle_call clause (4-tuple format)
- [ ] Add consumer group validation if needed
- [ ] Write infrastructure unit tests (aim for 20-30 tests)

### Phase 4: Backward Compatibility
- [ ] Update `client_compatibility.ex` with legacy handle_call (2-tuple)
- [ ] Verify adapter.ex works correctly (usually no changes needed)
- [ ] Test legacy API → new API interop
- [ ] Test new API → legacy API interop
- [ ] Run full legacy test suite (aim for >95% pass rate)

### Phase 5: Public API
- [ ] Add functions to `kafka_ex_api.ex`
- [ ] Write comprehensive @doc documentation
- [ ] Add API version comparison tables
- [ ] Provide usage examples
- [ ] Write unit tests (aim for 10-15 tests)

### Phase 6: Integration Tests
- [ ] Write integration tests for new API (10-15 tests)
- [ ] Write integration tests for GenServer.call interface (10-15 tests)
- [ ] Write compatibility tests (5-10 tests)
- [ ] Test with real Kafka cluster
- [ ] Test all API versions

### Phase 7: Documentation
- [ ] Update migration status document
- [ ] Update kayrock.md with API details
- [ ] Update new_api.md with examples
- [ ] Update README.md
- [ ] Update CHANGELOG.md

### Phase 8: Cleanup
- [ ] Run `mix credo` and fix warnings
- [ ] Run `mix dialyzer` and fix type issues
- [ ] Code review
- [ ] Performance testing
- [ ] Final integration testing

---

## Success Metrics

### Per-Migration Goals

- **Test Coverage:** 100% of new code
- **Integration Tests:** Minimum 25 tests per API
- **Legacy Compatibility:** >95% existing tests passing
- **Documentation:** Complete @doc for all public functions
- **Code Quality:** Zero credo/dialyzer warnings

### Overall Migration Goals

- **Timeline:** Complete Phase 1 (Consumer Groups) by Q1 2025
- **Quality:** Maintain backward compatibility throughout
- **Performance:** No performance regression vs adapter-only
- **Maintainability:** Reduce code duplication by 30%

---

## Known Patterns & Best Practices

### 1. Struct Consolidation
**Pattern:** Reuse existing structs instead of creating new ones
**Example:** OffsetFetch/OffsetCommit reuse `Offset` struct with pattern guards
**Benefit:** Zero duplication, single source of truth

### 2. Fail-Fast Error Handling
**Pattern:** Stop processing on first partition/topic error
**Example:** All offset APIs use `fail_fast_iterate_topics/2`
**Benefit:** Predictable error behavior, easier debugging

### 3. Version-Specific Parameters
**Pattern:** RequestBuilder adds fields based on API version
**Example:** OffsetCommit v2 adds `retention_time`, removes `timestamp`
**Benefit:** Clean separation of version concerns

### 4. Default API Versions
**Pattern:** Choose production-ready defaults (usually v1 or v2)
**Example:** OffsetFetch defaults to v1, OffsetCommit to v2
**Benefit:** Good defaults for most users, explicit opt-in for v0 or v3+

### 5. Pattern Matching Disambiguation
**Pattern:** Use tuple arity differences (2-tuple vs 4-tuple)
**Example:** `{:offset_fetch, request}` vs `{:offset_fetch, group, topics, opts}`
**Benefit:** Natural Elixir patterns, no routing complexity

---

## Test File Patterns

### Protocol Test Structure

All protocol tests follow a **consolidated pattern** with exactly two files per protocol:

```
test/kafka_ex/new/protocols/kayrock/{api_name}/
├── request_test.exs    # RequestHelpers tests + all version request tests
└── response_test.exs   # All version response tests
```

**DO NOT** create fragmented per-version test files like:
- ~~`v0_request_impl_test.exs`~~ (WRONG)
- ~~`v1_response_impl_test.exs`~~ (WRONG)
- ~~`request_helpers_test.exs`~~ (WRONG)

### Test File Structure

#### request_test.exs

```elixir
defmodule KafkaEx.New.Protocols.Kayrock.{ApiName}.RequestTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.{ApiName}
  alias KafkaEx.New.Protocols.Kayrock.{ApiName}.RequestHelpers

  # 1. RequestHelpers tests (if helpers exist)
  describe "RequestHelpers.extract_common_fields/1" do
    test "extracts required fields" do
      # ...
    end

    test "raises when required field is missing" do
      # ...
    end
  end

  # 2. V0 Request implementation tests
  describe "V0 Request implementation" do
    test "builds request with all required fields" do
      request = %Kayrock.{ApiName}.V0.Request{}
      opts = [field1: "value1", field2: "value2"]

      result = {ApiName}.Request.build_request(request, opts)

      assert result == %Kayrock.{ApiName}.V0.Request{
        client_id: nil,
        correlation_id: nil,
        field1: "value1",
        field2: "value2"
      }
    end

    test "preserves existing correlation_id and client_id" do
      # ...
    end
  end

  # 3. V1 Request implementation tests
  describe "V1 Request implementation" do
    test "builds request with version-specific fields" do
      # ...
    end
  end

  # 4. Continue for V2, V3, etc.
end
```

#### response_test.exs

```elixir
defmodule KafkaEx.New.Protocols.Kayrock.{ApiName}.ResponseTest do
  use ExUnit.Case, async: true

  alias KafkaEx.New.Protocols.Kayrock.{ApiName}
  alias KafkaEx.New.Kafka.{ApiName}, as: {ApiName}Struct  # if applicable

  # 1. V0 Response implementation tests
  describe "V0 Response implementation" do
    test "parses successful response with no error" do
      response = %Kayrock.{ApiName}.V0.Response{
        error_code: 0,
        # ... other fields
      }

      assert {:ok, result} = {ApiName}.Response.parse_response(response)
      # ... assertions
    end

    test "parses error response with specific error code" do
      response = %Kayrock.{ApiName}.V0.Response{
        error_code: 25  # e.g., unknown_member_id
      }

      assert {:error, error} = {ApiName}.Response.parse_response(response)
      assert error.error == :unknown_member_id
    end
  end

  # 2. V1 Response implementation tests
  describe "V1 Response implementation" do
    test "parses response with throttle_time_ms" do  # if V1 adds throttle
      # ...
    end
  end

  # 3. Version comparison tests (optional but recommended)
  describe "Version comparison" do
    test "V1 returns struct while V0 returns atom" do
      # ...
    end

    test "error responses are identical between versions" do
      # ...
    end
  end
end
```

### Test Count Guidelines

| Layer                         | Expected Tests          |
|-------------------------------|-------------------------|
| Protocol (request + response) | for each api version    |
| Struct tests                  | for each supported case |
| Adapter tests                 | for each supported case |
| Public API tests              | for each supported case |
| Integration tests             | for each supported case             |

### Test Naming Conventions

- Use descriptive test names that explain what is being tested
- Include version numbers in describe blocks: `"V0 Response implementation"`
- Group related tests in describe blocks
- Test both success and error paths
- Test edge cases (empty lists, nil values, large numbers)

### Example Reference Files

Use these as templates when writing new protocol tests:

1. **Simple protocol (no helpers):** `test/kafka_ex/new/protocols/kayrock/heartbeat/`
2. **Protocol with helpers:** `test/kafka_ex/new/protocols/kayrock/offset_commit/`
3. **Multi-version protocol:** `test/kafka_ex/new/protocols/kayrock/produce/`

---

## Resources

### Reference Implementations

1. **ListOffsets** - Best reference for offset-related APIs
   - Location: `lib/kafka_ex/new/protocols/kayrock/list_offsets/`
   - Pattern: Clean protocol layer, comprehensive tests
   - Status: ✅ Complete, merged to master

2. **OffsetFetch/OffsetCommit** - Best reference for dual API pattern
   - Location: `lib/kafka_ex/new/protocols/kayrock/offset_{fetch,commit}/`
   - Pattern: Pattern matching disambiguation, backward compatibility
   - Status: ✅ Complete (branch: KAYROCK-migrate-offset)

3. **DescribeGroups** - Best reference for consumer group introspection
   - Location: `lib/kafka_ex/new/protocols/kayrock/describe_groups/`
   - Pattern: Multiple API versions, group metadata handling
   - Status: ✅ Complete

4. **SyncGroup** - Best reference for consumer group coordination
   - Location: `lib/kafka_ex/new/protocols/kayrock/sync_group/`
   - Pattern: Group assignment synchronization, V0/V1 throttling
   - Status: ✅ Complete (branch: KAYROCK-migrate-sync-group)
   - Branch: `KAYROCK-migrate-sync-group`

### Documentation

- **Kafka Protocol Spec:** https://kafka.apache.org/protocol.html
- **Kayrock Docs:** https://hexdocs.pm/kayrock/
- **KafkaEx Docs:** https://hexdocs.pm/kafka_ex/

### Tools

- **Protocol Testing:** Use docker-compose Kafka cluster for integration tests
- **Code Quality:** `mix credo`, `mix dialyzer`, `mix format`
- **Test Runner:** `mix test --include integration`

---

## Completed Migrations

### Metadata (Completed: 2025-11-07)

**Branch:** `KAYROCK-next`

**Summary:**
Complete migration of Metadata API to Kayrock protocol layer with full backward compatibility. Implements V0, V1, and V2 protocol versions with comprehensive test coverage across all layers. This is a critical infrastructure migration as metadata is used by ALL operations in KafkaEx for broker discovery, topic routing, and partition assignment.

**Implementation Details:**
- **Protocol Layer:** V0, V1, and V2 request/response handlers
  - V0: Basic metadata (topics, partitions, brokers)
  - V1: Adds controller_id, is_internal flag, broker rack
  - V2: Adds cluster_id field
- **Data Structures:** Created comprehensive Metadata struct with helper functions for topic/broker queries
- **Infrastructure:** Full integration with RequestBuilder, ResponseParser, and Client
- **Backward Compatibility:** Legacy API support via Adapter (already existed, verified working)
- **Public API:** Clean interface in `KafkaEx.New.KafkaExAPI` with `metadata/1,2,3` and enhanced `cluster_metadata/1`

**Statistics:**
- **Files Changed:** 7 files
- **Lines Added:** ~1,450 insertions
- **Test Count:** 125 new tests + 1,068 legacy tests passing (100%)
  - Struct tests: 36 tests (request building, response helpers, edge cases)
  - Protocol tests: 32 tests (V0/V1/V2 request and response implementations)
  - Infrastructure tests: 14 tests (RequestBuilder, ResponseParser integration)
  - Adapter tests: 9 tests (legacy format conversion)
  - Public API tests: 17 tests (MockClient-based unit tests)
  - Integration tests: 17 tests (real Kafka cluster)
  - All unit tests: 1,068/1,068 passing ✅
  - All integration tests: Pass individually ✅

**Key Learnings:**
1. **Pattern Matching Disambiguation:** Used `when is_atom(key)` guard to distinguish keyword lists from regular lists
2. **API Version Selection:** V1 is the best default (adds controller_id and is_internal, widely supported)
3. **Broker Selection:** Metadata can be fetched from any broker (use NodeSelector.random())
4. **State Management:** Metadata responses automatically update client's cluster_metadata state
5. **Topic Filtering:** Response parser filters out topics/partitions with error_code != 0

**Files Created:**
- `lib/kafka_ex/new/structs/metadata.ex` - Request/Response structs with helpers
- `lib/kafka_ex/new/protocols/kayrock/metadata.ex` - Protocol definitions
- `lib/kafka_ex/new/protocols/kayrock/metadata/request_helpers.ex` - Request building helpers
- `lib/kafka_ex/new/protocols/kayrock/metadata/response_helpers.ex` - Response parsing helpers
- `lib/kafka_ex/new/protocols/kayrock/metadata/v{0,1,2}_{request,response}_impl.ex` - Version implementations
- `test/kafka_ex/new/structs/metadata_test.exs` - Struct tests
- `test/kafka_ex/new/protocols/kayrock/metadata/request_test.exs` - Request protocol tests
- `test/kafka_ex/new/protocols/kayrock/metadata/response_test.exs` - Response protocol tests
- `test/kafka_ex/new/client/metadata_integration_test.exs` - Infrastructure tests
- `test/kafka_ex/new/adapter_metadata_test.exs` - Adapter conversion tests
- `test/kafka_ex/new/kafka_ex_api_metadata_test.exs` - Public API unit tests

**Files Modified:**
- `lib/kafka_ex/new/protocols/kayrock_protocol.ex` - Added metadata handlers
- `lib/kafka_ex/new/client/request_builder.ex` - Added metadata_request/2
- `lib/kafka_ex/new/client/response_parser.ex` - Added metadata_response/1
- `lib/kafka_ex/new/client.ex` - Added 4-tuple handle_call for metadata
- `lib/kafka_ex/new/kafka_ex_api.ex` - Added metadata/1,2,3 functions, enhanced cluster_metadata/1
- `test/integration/kafka_ex_api_test.exs` - Added 17 integration tests
- `test/kafka_ex/new/structs/consumer_group/member_test.exs` - Fixed syntax error

**Public API Examples:**
```elixir
# Fetch metadata for all topics (default V1)
{:ok, metadata} = KafkaEx.New.KafkaExAPI.metadata(client)

# Fetch metadata with specific API version
{:ok, metadata} = KafkaEx.New.KafkaExAPI.metadata(client, api_version: 2)

# Fetch metadata for specific topics
{:ok, metadata} = KafkaEx.New.KafkaExAPI.metadata(client, ["orders", "payments"], [])

# Get cached cluster metadata (no network request)
{:ok, cached} = KafkaEx.New.KafkaExAPI.cluster_metadata(client)

# Access broker and topic information
controller_id = metadata.controller_id
brokers = metadata.brokers
topic = metadata.topics["orders"]
partition_leaders = topic.partition_leaders  # %{0 => 1, 1 => 2, 2 => 3}
```

**Migration Checklist:**
- [x] Phase 1: Data Structures (36 tests)
- [x] Phase 2: Protocol Implementation (32 tests - V0, V1, V2)
- [x] Phase 3: Infrastructure Integration (14 tests)
- [x] Phase 4: Backward Compatibility (9 adapter tests, 1068 legacy tests)
- [x] Phase 5: Public API (17 tests)
- [x] Phase 6: Integration Tests (17 tests)
- [x] Phase 7: Documentation Updates (METADATA_MIGRATION.md)
- [x] Phase 8: Code Quality (all tests passing, no warnings)

---

### ApiVersions (Completed: 2025-12-09)

**Branch:** `master`

**Summary:**
Complete migration of ApiVersions API to Kayrock protocol layer with full backward compatibility. Implements V0 and V1 protocol versions with comprehensive test coverage. This is a foundational infrastructure migration that enables dynamic API version negotiation for all other Kafka operations.

**Implementation Details:**
- **Protocol Layer:** V0 and V1 request/response handlers
  - V0: Basic API version discovery (no throttle_time_ms)
  - V1: Adds throttle_time_ms for rate limiting visibility
- **Data Structures:** Created ApiVersions struct with helper functions for version lookups
  - `max_version_for_api/2` - Find max supported version for an API
  - `min_version_for_api/2` - Find min supported version for an API
  - `version_supported?/3` - Check if specific version is supported
- **Infrastructure:** Full integration with RequestBuilder, ResponseParser, and Client
- **Backward Compatibility:** Legacy API continues working via existing Adapter (no changes needed)
- **Public API:** Clean interface in `KafkaEx.New.KafkaExAPI.api_versions/1,2`

**Statistics:**
- **Files Created:** 6 files
- **Files Modified:** 5 files
- **Lines Added:** ~400 insertions
- **Test Count:** 21 tests across all layers
  - Adapter tests: 9 tests (legacy format conversion)
  - Integration tests: 12 tests (real Kafka cluster)
  - All tests passing ✅

**Key Learnings:**
1. **Simple Migration Pattern:** ApiVersions has no parameters, making it an ideal simple migration
2. **Map-Based Storage:** Using `%{api_key => %{min_version, max_version}}` for O(1) lookups
3. **Backward Compatibility:** Existing adapter/compatibility layer worked without changes
4. **Helper Functions:** Version lookup helpers provide excellent developer experience

**Files Created:**
- `lib/kafka_ex/new/structs/api_versions.ex` - Struct with helper functions
- `lib/kafka_ex/new/protocols/kayrock/api_versions.ex` - Protocol definitions
- `lib/kafka_ex/new/protocols/kayrock/api_versions/v0_request_impl.ex`
- `lib/kafka_ex/new/protocols/kayrock/api_versions/v0_response_impl.ex`
- `lib/kafka_ex/new/protocols/kayrock/api_versions/v1_request_impl.ex`
- `lib/kafka_ex/new/protocols/kayrock/api_versions/v1_response_impl.ex`
- `test/kafka_ex/new/adapter_api_versions_test.exs` - Adapter tests

**Files Modified:**
- `lib/kafka_ex/new/protocols/kayrock_protocol.ex` - Added api_versions handlers
- `lib/kafka_ex/new/client/request_builder.ex` - Added api_versions_request/2
- `lib/kafka_ex/new/client/response_parser.ex` - Added api_versions_response/1
- `lib/kafka_ex/new/client.ex` - Added handle_call for api_versions
- `lib/kafka_ex/new/kafka_ex_api.ex` - Added api_versions/1,2 functions
- `test/integration/kafka_ex_api_test.exs` - Added 12 integration tests

**Public API Examples:**
```elixir
# Fetch API versions (default V0)
{:ok, versions} = KafkaEx.New.KafkaExAPI.api_versions(client)

# Fetch with V1 to get throttle information
{:ok, versions} = KafkaEx.New.KafkaExAPI.api_versions(client, api_version: 1)

# Find max version for Metadata API (key 3)
{:ok, max_version} = ApiVersions.max_version_for_api(versions, 3)
# => {:ok, 2}

# Check if a specific version is supported
ApiVersions.version_supported?(versions, 3, 1)
# => true

# Check for unsupported API
ApiVersions.max_version_for_api(versions, 999)
# => {:error, :unsupported_api}
```

**Migration Checklist:**
- [x] Phase 1: Data Structures
- [x] Phase 2: Protocol Implementation (V0, V1)
- [x] Phase 3: Infrastructure Integration
- [x] Phase 4: Backward Compatibility (9 adapter tests)
- [x] Phase 5: Public API
- [x] Phase 6: Integration Tests (12 tests)
- [x] Phase 7: Documentation Updates
- [x] Phase 8: Code Quality (format, credo passing)

---

### JoinGroup (Completed: 2025-11-04)

**Branch:** `KAYROCK-migrate-join-group`

**Summary:**
Complete migration of JoinGroup API to Kayrock protocol layer with full backward compatibility. Implements V0, V1, and V2 protocol versions with comprehensive test coverage. This completes the core consumer group coordination protocol, enabling full consumer group rebalancing workflows.

**Implementation Details:**
- **Protocol Layer:** V0, V1, and V2 request/response handlers
  - V0: Basic JoinGroup with session_timeout
  - V1: Adds rebalance_timeout field (decouples rebalance from session timeout)
  - V2: Adds throttle_time_ms to response for rate limiting visibility
- **Infrastructure:** Full integration with RequestBuilder, ResponseParser, and Client
- **Backward Compatibility:** Legacy API support via Adapter and ClientCompatibility (already existed)
- **Public API:** Comprehensive interface in `KafkaEx.New.KafkaExAPI.join_group/3,4` with detailed documentation

**Statistics:**
- **Files Changed:** 12 files (5 infrastructure + 7 test files)
- **Lines Added:** ~350 insertions (infrastructure only, protocol layer pre-existing)
- **Lines Removed:** 7 deletions (test fixes)
- **Test Count:** 42 protocol tests + 518 total unit tests
  - Protocol tests: V0/V1/V2 request and response implementations (15 test files)
  - Struct tests: Data structure validation (230 tests)
  - Infrastructure tests: RequestBuilder, ResponseParser integration
  - All tests passing with 0 failures ✅

**Key Learnings:**
1. **Version Support:** Kayrock only supports V0-V2, not V0-V5 as initially documented
2. **Rebalance Timeout:** V1+ separates rebalance_timeout from session_timeout for better control
3. **Leader Election:** Use `JoinGroup.leader?/1` to determine if member is group leader
4. **Member Metadata:** Leaders receive all members' metadata for partition assignment computation
5. **First Join:** Use empty string `""` for member_id on first join

**Files Modified:**
- Protocol implementations: `lib/kafka_ex/new/protocols/kayrock/join_group/*.ex` (pre-existing)
- Struct definition: `lib/kafka_ex/new/structs/join_group.ex` (pre-existing)
- Infrastructure: `kayrock_protocol.ex`, `request_builder.ex`, `response_parser.ex`, `client.ex`
- Public API: `kafka_ex_api.ex`
- Tests: 7 test files fixed (error field assertions)

**Consumer Group Workflow Integration:**
```elixir
# 1. Join the group
{:ok, join_response} = KafkaEx.New.KafkaExAPI.join_group(
  client, "my-group", "",
  session_timeout: 30_000,
  rebalance_timeout: 60_000,
  group_protocols: [...]
)

# 2. Leader computes assignments, followers wait
if JoinGroup.leader?(join_response) do
  assignments = compute_partition_assignments(join_response.members)
  {:ok, sync_response} = KafkaEx.New.KafkaExAPI.sync_group(
    client, "my-group", join_response.generation_id,
    join_response.member_id, group_assignment: assignments
  )
else
  {:ok, sync_response} = KafkaEx.New.KafkaExAPI.sync_group(
    client, "my-group", join_response.generation_id,
    join_response.member_id, group_assignment: []
  )
end
```

**Migration Checklist:**
- [x] Phase 1: Data Structures
- [x] Phase 2: Protocol Implementation (V0, V1, V2)
- [x] Phase 3: Infrastructure Integration
- [x] Phase 4: Backward Compatibility
- [x] Phase 5: Public API
- [x] Phase 6: Integration Tests (requires Kafka cluster)
- [x] Phase 7: Documentation Updates
- [x] Phase 8: Code Quality (format, credo passing)

---

### SyncGroup (Completed: 2025-10-22)

**Branch:** `KAYROCK-migrate-sync-group`

**Summary:**
Complete migration of SyncGroup API to Kayrock protocol layer with full backward compatibility. Implements both V0 and V1 protocol versions with comprehensive test coverage.

**Implementation Details:**
- **Protocol Layer:** V0 and V1 request/response handlers
  - V0: Basic group synchronization
  - V1: Adds throttle_time_ms support
- **Infrastructure:** Full integration with RequestBuilder, ResponseParser, and Client
- **Backward Compatibility:** Legacy API support via Adapter and ClientCompatibility
- **Public API:** Clean interface in `KafkaEx.New.KafkaExAPI.sync_group/4,5`

**Statistics:**
- **Files Changed:** 28 files
- **Lines Added:** 2,450 insertions
- **Lines Removed:** 33 deletions
- **Test Count:** ~79 tests across all layers
  - Protocol tests: V0/V1 request and response implementations
  - Adapter tests: Legacy to new API conversion (45+ tests)
  - Struct tests: Data structure validation
  - Integration tests: Real Kafka cluster scenarios

**Key Learnings:**
1. **Assignment Structure:** Member assignments use nested list format: `[{member_id, [{topic, partitions}]}]`
2. **MemberAssignment Type:** Always use `Kayrock.MemberAssignment` struct for proper type safety
3. **Throttle Handling:** V1 includes throttle_time_ms but it's optional in response struct
4. **Consumer Group Validation:** SyncGroup requires valid consumer group validation before processing

**Files Modified:**
- Protocol implementations: `lib/kafka_ex/new/protocols/kayrock/sync_group/*.ex`
- Struct definition: `lib/kafka_ex/new/structs/sync_group.ex`
- Infrastructure: `kayrock_protocol.ex`, `request_builder.ex`, `response_parser.ex`, `client.ex`
- Adapter: `adapter.ex`, `client_compatibility.ex`
- Public API: `kafka_ex_api.ex`
- Tests: 8 new test files with comprehensive coverage

**Migration Checklist:**
- [x] Phase 1: Data Structures
- [x] Phase 2: Protocol Implementation (V0, V1)
- [x] Phase 3: Infrastructure Integration
- [x] Phase 4: Backward Compatibility
- [x] Phase 5: Public API
- [x] Phase 6: Integration Tests (requires Kafka cluster)
- [x] Phase 7: Documentation Updates
- [x] Phase 8: Code Quality (format, credo passing)

---

### Produce (Completed: 2025-12-11)

**Branch:** `KAY-migrate-produce-endpoint`

**Summary:**
Complete migration of Produce API to Kayrock protocol layer with full backward compatibility. Implements V0 through V5 protocol versions with comprehensive test coverage. This is a critical migration for message publishing, supporting both legacy MessageSet (V0-V2) and modern RecordBatch (V3+) message formats.

**Implementation Details:**
- **Protocol Layer:** V0, V1, V2, V3, V4, and V5 request/response handlers
  - V0: Basic produce with base_offset only
  - V1: Adds throttle_time_ms for rate limiting visibility
  - V2: Adds log_append_time (LogAppendTime vs CreateTime)
  - V3: RecordBatch format with headers support
  - V4: Same as V3 (RecordBatch format)
  - V5: Adds log_start_offset for log compaction visibility
- **Message Formats:**
  - V0-V2: MessageSet format (legacy, simpler structure)
  - V3+: RecordBatch format (headers, timestamps, transactional support)
- **Compression:** Supports :none, :gzip, :snappy, :lz4, :zstd
- **Data Structures:** Created Produce struct with all response fields
- **Infrastructure:** Full integration with RequestBuilder, ResponseParser, and Client
- **Backward Compatibility:** Legacy API continues working via existing Adapter
- **Public API:** Clean interface with `produce/4,5` and `produce_one/4,5` convenience functions

**Statistics:**
- **Files Created:** 18 files
- **Files Modified:** 5 files
- **Lines Added:** ~2,000 insertions
- **Test Count:** 143 tests across all layers
  - Struct tests: 9 tests (data structure validation)
  - Protocol request tests: 24 tests (V0-V5 request building)
  - Protocol response tests: 20 tests (V0-V5 response parsing)
  - Adapter tests: 17 tests (legacy format conversion)
  - Public API tests: 24 tests (MockClient-based unit tests)
  - Integration tests: 37 tests (real Kafka cluster)
  - Compatibility tests: 12 tests (new/legacy API interop)
  - All tests passing ✅

**Key Learnings:**
1. **Message Format Evolution:** V0-V2 use MessageSet, V3+ use RecordBatch with fundamentally different structures
2. **Compression Scope:** MessageSet compresses individual messages, RecordBatch compresses the entire batch
3. **Headers Support:** Only available in V3+ (RecordBatch format)
4. **Timestamp Handling:** V2+ supports custom timestamps, broker may override with LogAppendTime
5. **Response Fields:** Progressive addition of fields across versions requires version-specific parsing

**Files Created:**
- `lib/kafka_ex/new/structs/produce.ex` - Produce struct with response fields
- `lib/kafka_ex/new/protocols/kayrock/produce.ex` - Protocol definitions
- `lib/kafka_ex/new/protocols/kayrock/produce/request_helpers.ex` - Request building helpers
- `lib/kafka_ex/new/protocols/kayrock/produce/response_helpers.ex` - Response parsing helpers
- `lib/kafka_ex/new/protocols/kayrock/produce/v{0,1,2,3,4,5}_request_impl.ex` - Request implementations
- `lib/kafka_ex/new/protocols/kayrock/produce/v{0,1,2,3,4,5}_response_impl.ex` - Response implementations
- `test/kafka_ex/new/structs/produce_test.exs` - Struct tests
- `test/kafka_ex/new/protocols/kayrock/produce/request_test.exs` - Request protocol tests
- `test/kafka_ex/new/protocols/kayrock/produce/response_test.exs` - Response protocol tests
- `test/kafka_ex/new/adapter/produce_test.exs` - Adapter conversion tests
- `test/kafka_ex/new/kafka_ex_api_produce_test.exs` - Public API unit tests
- `test/integration/produce_kayrock_test.exs` - Integration tests

**Files Modified:**
- `lib/kafka_ex/new/protocols/kayrock_protocol.ex` - Added produce handlers
- `lib/kafka_ex/new/client/request_builder.ex` - Added produce_request/2
- `lib/kafka_ex/new/client/response_parser.ex` - Added produce_response/1
- `lib/kafka_ex/new/client.ex` - Added handle_call for produce
- `lib/kafka_ex/new/kafka_ex_api.ex` - Added produce/4,5 and produce_one/4,5
- `test/integration/kayrock/compatibility_test.exs` - Added 12 produce compatibility tests

**Public API Examples:**
```elixir
# Produce multiple messages
messages = [
  %{value: "message 1", key: "key1"},
  %{value: "message 2", key: "key2"}
]
{:ok, result} = KafkaEx.New.KafkaExAPI.produce(client, "my-topic", 0, messages)
# => %Produce{topic: "my-topic", partition: 0, base_offset: 42, ...}

# Produce with options
{:ok, result} = KafkaEx.New.KafkaExAPI.produce(client, "my-topic", 0, messages,
  acks: -1,              # Wait for all ISR
  timeout: 10_000,       # 10 second timeout
  compression: :gzip,    # Compress messages
  api_version: 3         # Use RecordBatch format
)

# Produce single message (convenience function)
{:ok, result} = KafkaEx.New.KafkaExAPI.produce_one(client, "my-topic", 0, "hello world")

# Produce with message-specific options
{:ok, result} = KafkaEx.New.KafkaExAPI.produce_one(client, "my-topic", 0, "event data",
  key: "event-id",
  timestamp: System.system_time(:millisecond),
  headers: [{"content-type", "application/json"}],  # V3+ only
  compression: :snappy,
  api_version: 3
)

# Access response fields
result.base_offset        # Offset assigned to first message
result.log_append_time    # Broker timestamp (V2+, may be -1)
result.throttle_time_ms   # Rate limiting info (V1+)
result.log_start_offset   # Log start for compaction (V5+)
```

**Migration Checklist:**
- [x] Phase 1: Data Structures (9 tests)
- [x] Phase 2: Protocol Implementation (44 tests - V0, V1, V2, V3, V4, V5)
- [x] Phase 3: Infrastructure Integration
- [x] Phase 4: Backward Compatibility (17 adapter tests)
- [x] Phase 5: Public API (24 tests)
- [x] Phase 6: Integration Tests (49 tests)
- [x] Phase 7: Documentation Updates
- [x] Phase 8: Code Quality (format, credo pending)

---

### Fetch (Completed: 2025-12-12)

**Branch:** `KAY-migrate-produce-endpoint`

**Summary:**
Complete migration of Fetch API to Kayrock protocol layer with full backward compatibility. Implements V0 through V7 protocol versions with comprehensive test coverage. This is the counterpart to Produce, enabling complete message consumption through the new Kayrock-based client. The migration handles both legacy MessageSet (V0-V3) and modern RecordBatch (V4+) message formats.

**Implementation Details:**
- **Protocol Layer:** V0, V1, V2, V3, V4, V5, V6, and V7 request/response handlers
  - V0: Basic fetch using MessageSet format
  - V1: Adds throttle_time_ms for rate limiting visibility
  - V2: Same as V1
  - V3: Adds max_bytes at request level
  - V4: Adds isolation_level, last_stable_offset, aborted_transactions (transactional reads)
  - V5: Adds log_start_offset in request and response
  - V6: Same as V5
  - V7: Adds incremental fetch (session_id, epoch, forgotten_topics_data)
- **Message Formats:**
  - V0-V3: May return MessageSet format (legacy)
  - V4+: Returns RecordBatch format (modern format with headers)
- **Data Structures:** Created Fetch and Fetch.Message structs with helper functions
- **Infrastructure:** Full integration with RequestBuilder, ResponseParser, and Client
- **Public API:** Clean interface with `fetch/4,5` and `fetch_all/3,4` convenience functions

**Statistics:**
- **Files Created:** 22 files
- **Files Modified:** 6 files
- **Lines Added:** ~2,100 insertions
- **Test Count:** 172 tests across all layers
  - Struct tests (Fetch): 9 tests
  - Struct tests (Message): 13 tests
  - Request helpers tests: 15 tests
  - Response helpers tests: 16 tests
  - Request builder tests: 9 tests
  - Adapter tests: 21 tests
  - Integration tests: 35 tests
  - All tests passing ✅

**Key Learnings:**
1. **Message Format Evolution:** V0-V3 may return MessageSet, V4+ returns RecordBatch
2. **Isolation Levels:** V4+ supports READ_UNCOMMITTED (0) and READ_COMMITTED (1)
3. **Incremental Fetch:** V7+ supports session-based fetching for reduced bandwidth
4. **Aborted Transactions:** V4+ includes aborted transaction info for transactional consumers
5. **Response Parser:** Handles both MessageSet and RecordBatch formats transparently

**Files Created:**
- `lib/kafka_ex/new/kafka/fetch.ex` - Fetch struct with response fields
- `lib/kafka_ex/new/kafka/fetch/message.ex` - Message struct with helper functions
- `lib/kafka_ex/new/protocols/kayrock/fetch.ex` - Protocol definitions
- `lib/kafka_ex/new/protocols/kayrock/fetch/request_helpers.ex` - Request building helpers
- `lib/kafka_ex/new/protocols/kayrock/fetch/response_helpers.ex` - Response parsing helpers
- `lib/kafka_ex/new/protocols/kayrock/fetch/v{0,1,2,3,4,5,6,7}_request_impl.ex` - Request implementations
- `lib/kafka_ex/new/protocols/kayrock/fetch/v{0,1,2,3,4,5,6,7}_response_impl.ex` - Response implementations
- `test/kafka_ex/new/structs/fetch_test.exs` - Fetch struct tests
- `test/kafka_ex/new/structs/fetch_message_test.exs` - Message struct tests
- `test/kafka_ex/new/protocols/kayrock/fetch/request_helpers_test.exs` - Request helpers tests
- `test/kafka_ex/new/protocols/kayrock/fetch/response_helpers_test.exs` - Response helpers tests
- `test/kafka_ex/new/adapter/fetch_test.exs` - Adapter conversion tests
- `test/integration/fetch_kayrock_test.exs` - Integration tests

**Files Modified:**
- `lib/kafka_ex/new/protocols/kayrock_protocol.ex` - Added fetch handlers
- `lib/kafka_ex/new/client/request_builder.ex` - Added fetch_request/2
- `lib/kafka_ex/new/client/response_parser.ex` - Added fetch_response/1
- `lib/kafka_ex/new/client.ex` - Added handle_call for fetch
- `lib/kafka_ex/new/kafka_ex_api.ex` - Added fetch/4,5 and fetch_all/3,4

**Public API Examples:**
```elixir
# Fetch messages from a topic partition
{:ok, result} = KafkaEx.New.KafkaExAPI.fetch(client, "my-topic", 0, 0)
# => %Fetch{topic: "my-topic", partition: 0, messages: [...], high_watermark: 100}

# Fetch with options
{:ok, result} = KafkaEx.New.KafkaExAPI.fetch(client, "my-topic", 0, 100,
  max_bytes: 500_000,      # Max bytes per partition
  max_wait_time: 5_000,    # 5 second wait
  min_bytes: 100,          # Min bytes before returning
  isolation_level: 1,      # READ_COMMITTED (V4+)
  api_version: 5           # Use V5 with log_start_offset
)

# Fetch all messages from earliest offset
{:ok, result} = KafkaEx.New.KafkaExAPI.fetch_all(client, "my-topic", 0)

# Access response fields
result.topic              # Topic name
result.partition          # Partition number
result.messages           # List of Message structs
result.high_watermark     # High watermark offset
result.last_offset        # Last fetched offset
result.last_stable_offset # Last stable offset (V4+)
result.log_start_offset   # Log start offset (V5+)
result.throttle_time_ms   # Throttle time (V1+)

# Access individual messages
for msg <- result.messages do
  msg.offset
  msg.key
  msg.value
  msg.timestamp
  msg.headers  # V4+ only (RecordBatch format)
end

# Helper functions
Fetch.empty?(result)        # Check if no messages
Fetch.message_count(result) # Count messages
Fetch.next_offset(result)   # Get next offset to fetch

# Message helpers
Message.has_value?(msg)
Message.has_key?(msg)
Message.has_headers?(msg)
Message.get_header(msg, "content-type")
```

**Migration Checklist:**
- [x] Phase 1: Data Structures (Fetch + Message structs, 22 tests)
- [x] Phase 2: Protocol Implementation (V0-V7 request/response, 16 impl files)
- [x] Phase 3: Infrastructure Integration (RequestBuilder, ResponseParser, KayrockProtocol)
- [x] Phase 4: Backward Compatibility (existing Adapter handles legacy, 21 adapter tests)
- [x] Phase 5: Public API (fetch/4,5 and fetch_all/3,4)
- [x] Phase 6: Unit Tests (137 tests passing)
- [x] Phase 7: Integration Tests (35 tests passing)
- [x] Phase 8: Code Quality (format, dialyzer, credo passing)



---

### FindCoordinator (Completed: 2025-12-12)

**Summary:**
Complete migration of FindCoordinator API to Kayrock protocol layer. This API discovers the coordinator broker for a consumer group (type=0) or transactional producer (type=1). Implements V0 and V1 protocol versions with comprehensive test coverage.

**Implementation Details:**
- **Protocol Layer:** V0 and V1 request/response handlers
  - V0: Basic group coordinator discovery using `group_id`
  - V1: Extended discovery with `coordinator_key`, `coordinator_type`, `throttle_time_ms`, `error_message`
- **Data Structures:** Created `FindCoordinator` struct with coordinator broker info and helper functions
- **Infrastructure:** Full integration with RequestBuilder, ResponseParser, KayrockProtocol, and Client
- **Public API:** Clean interface in `KafkaEx.New.KafkaExAPI` with `find_coordinator/2,3`

**Statistics:**
- **Files Created:** 14 files
- **Lines Added:** ~1,100 insertions
- **Test Count:** 91 new tests + 1752 unit tests passing (100%)
  - Protocol tests: 27 tests (V0/V1 request and response implementations)
  - Struct tests: 11 tests (FindCoordinator struct, helper functions)
  - Infrastructure tests: 7 tests (RequestBuilder, ResponseParser integration)
  - Adapter tests: 25 tests (legacy ConsumerMetadata format conversion)
  - ClientCompatibility tests: 8 tests (legacy handle_call)
  - Integration tests: 10 tests (real Kafka cluster)
  - Compatibility tests: 5 tests (new/legacy API interop)

**Key Learnings:**
1. **Coordinator Types:** V1 supports both group (0) and transaction (1) coordinators
2. **API Key:** FindCoordinator uses API key 10 in Kayrock
3. **Broker Selection:** FindCoordinator can be sent to any broker (use NodeSelector.first_available())
4. **Error Codes:** Common errors include coordinator_not_available (15), not_coordinator (16), invalid_group_id (24)

**Files Created:**
- `lib/kafka_ex/new/kafka/find_coordinator.ex` - FindCoordinator struct with helper functions
- `lib/kafka_ex/new/protocols/kayrock/find_coordinator.ex` - Protocol definitions
- `lib/kafka_ex/new/protocols/kayrock/find_coordinator/request_helpers.ex` - Request building helpers
- `lib/kafka_ex/new/protocols/kayrock/find_coordinator/response_helpers.ex` - Response parsing helpers
- `lib/kafka_ex/new/protocols/kayrock/find_coordinator/v0_request_impl.ex` - V0 request implementation
- `lib/kafka_ex/new/protocols/kayrock/find_coordinator/v0_response_impl.ex` - V0 response implementation
- `lib/kafka_ex/new/protocols/kayrock/find_coordinator/v1_request_impl.ex` - V1 request implementation
- `lib/kafka_ex/new/protocols/kayrock/find_coordinator/v1_response_impl.ex` - V1 response implementation
- `test/kafka_ex/new/protocols/kayrock/find_coordinator/request_test.exs` - Request protocol tests
- `test/kafka_ex/new/protocols/kayrock/find_coordinator/response_test.exs` - Response protocol tests
- `test/kafka_ex/new/structs/find_coordinator_test.exs` - Struct tests
- `test/kafka_ex/new/kafka_ex_api_find_coordinator_test.exs` - API tests
- `test/kafka_ex/new/adapter/find_coordinator_test.exs` - Adapter tests

**Files Modified:**
- `lib/kafka_ex/new/protocols/kayrock_protocol.ex` - Added find_coordinator handlers
- `lib/kafka_ex/new/client/request_builder.ex` - Added find_coordinator_request/2
- `lib/kafka_ex/new/client/response_parser.ex` - Added find_coordinator_response/1
- `lib/kafka_ex/new/client.ex` - Added handle_call for find_coordinator
- `lib/kafka_ex/new/kafka_ex_api.ex` - Added find_coordinator/2,3
- `lib/kafka_ex/new/adapter.ex` - Added consumer_metadata_request/1, consumer_metadata_response/1
- `lib/kafka_ex/new/client_compatibility.ex` - Added handle_call for {:consumer_group_metadata, _}
- `test/integration/kayrock/compatibility_test.exs` - Added 5 find_coordinator compatibility tests

**Public API Examples:**
```elixir
# Find group coordinator (default)
{:ok, result} = KafkaEx.New.KafkaExAPI.find_coordinator(client, "my-consumer-group")
# => %FindCoordinator{
#      coordinator: %Broker{node_id: 1, host: "broker1", port: 9092},
#      error_code: :no_error,
#      throttle_time_ms: 0
#    }

# Find transaction coordinator
{:ok, result} = KafkaEx.New.KafkaExAPI.find_coordinator(client, "my-transactional-id",
  coordinator_type: :transaction)

# Use V0 API (only supports group coordinators)
{:ok, result} = KafkaEx.New.KafkaExAPI.find_coordinator(client, "my-group",
  api_version: 0)

# Access coordinator info
coordinator = result.coordinator
coordinator.node_id  # Broker ID
coordinator.host     # Broker hostname
coordinator.port     # Broker port

# Helper functions
FindCoordinator.success?(result)           # Check if no error
FindCoordinator.coordinator_node_id(result) # Get coordinator node ID
```

**Migration Checklist:**
- [x] Phase 1: Data Structures (FindCoordinator struct, 11 tests)
- [x] Phase 2: Protocol Implementation (V0, V1 request/response, 27 tests)
- [x] Phase 3: Infrastructure Integration (RequestBuilder, ResponseParser, KayrockProtocol, Client)
- [x] Phase 4: Backward Compatibility (Adapter consumer_metadata_request/response, 25 tests)
- [x] Phase 5: Public API (find_coordinator/2,3, 7 tests)
- [x] Phase 6: Unit Tests (70 tests passing)
- [x] Phase 7: Integration Tests (10 tests passing)
- [x] Phase 8: Code Quality (format, credo passing)

---

### CreateTopics (Completed: 2025-12-15)

**Summary:**
Complete migration of CreateTopics API to Kayrock protocol layer with full backward compatibility. Implements V0, V1, and V2 protocol versions with comprehensive test coverage. This API allows creating new topics with specified partitions, replication factors, and configuration options.

**Implementation Details:**
- **Protocol Layer:** V0, V1, and V2 request/response handlers
  - V0: Basic topic creation with timeout
  - V1: Adds validate_only flag, error_message in response
  - V2: Adds throttle_time_ms in response
- **Data Structures:** Created CreateTopics struct with TopicResult nested struct
  - `success?/1` - Check if all topics created successfully
  - `failed_topics/1` - Get list of failed topic results
  - `successful_topics/1` - Get list of successful topic results
  - `get_topic_result/2` - Get result for a specific topic
- **Infrastructure:** Full integration with RequestBuilder, ResponseParser, KayrockProtocol, and Client
- **Public API:** Clean interface in `KafkaEx.New.KafkaExAPI` with `create_topics/4` and `create_topic/3`
- **Node Selection:** Uses `NodeSelector.controller()` as CreateTopics must be sent to the controller broker

**Statistics:**
- **Files Created:** 10 files
- **Files Modified:** 6 files
- **Lines Added:** ~1,200 insertions
- **Test Count:** 65 new tests + 1809 unit tests passing (100%)
  - Struct tests: 17 tests (CreateTopics and TopicResult structs)
  - Request protocol tests: 22 tests (RequestHelpers, V0/V1/V2 implementations)
  - Response protocol tests: 15 tests (ResponseHelpers, V0/V1/V2 implementations)
  - Integration tests: 11 tests (compatibility tests)

**Key Learnings:**
1. **Controller Broker:** CreateTopics API key 19 must be sent to the controller broker
2. **Error Messages:** V1+ includes error_message in response for better diagnostics
3. **Throttle Time:** V2+ includes throttle_time_ms for rate limiting visibility
4. **Validate Only:** V1+ supports dry-run validation with validate_only flag
5. **Default Values:** num_partitions and replication_factor default to -1 (use broker defaults)

**Files Created:**
- `lib/kafka_ex/new/kafka/create_topics.ex` - CreateTopics struct with TopicResult nested struct
- `lib/kafka_ex/new/protocols/kayrock/create_topics.ex` - Protocol definitions
- `lib/kafka_ex/new/protocols/kayrock/create_topics/request_helpers.ex` - Request building helpers
- `lib/kafka_ex/new/protocols/kayrock/create_topics/response_helpers.ex` - Response parsing helpers
- `lib/kafka_ex/new/protocols/kayrock/create_topics/v0_request_impl.ex` - V0 request implementation
- `lib/kafka_ex/new/protocols/kayrock/create_topics/v0_response_impl.ex` - V0 response implementation
- `lib/kafka_ex/new/protocols/kayrock/create_topics/v1_request_impl.ex` - V1 request implementation
- `lib/kafka_ex/new/protocols/kayrock/create_topics/v1_response_impl.ex` - V1 response implementation
- `lib/kafka_ex/new/protocols/kayrock/create_topics/v2_request_impl.ex` - V2 request implementation
- `lib/kafka_ex/new/protocols/kayrock/create_topics/v2_response_impl.ex` - V2 response implementation
- `test/kafka_ex/new/structs/create_topics_test.exs` - Struct tests
- `test/kafka_ex/new/protocols/kayrock/create_topics/request_test.exs` - Request protocol tests
- `test/kafka_ex/new/protocols/kayrock/create_topics/response_test.exs` - Response protocol tests

**Files Modified:**
- `lib/kafka_ex/new/protocols/kayrock_protocol.ex` - Added create_topics handlers
- `lib/kafka_ex/new/client/request_builder.ex` - Added create_topics_request/2
- `lib/kafka_ex/new/client/response_parser.ex` - Added create_topics_response/1
- `lib/kafka_ex/new/client.ex` - Added handle_call for create_topics
- `lib/kafka_ex/new/kafka_ex_api.ex` - Added create_topics/4 and create_topic/3
- `test/integration/kayrock/compatibility_test.exs` - Added 11 create_topics compatibility tests

**Public API Examples:**
```elixir
# Create a single topic with defaults
{:ok, result} = KafkaEx.New.KafkaExAPI.create_topic(client, "my-new-topic")

# Create a topic with specific partitions and replication
{:ok, result} = KafkaEx.New.KafkaExAPI.create_topic(client, "my-topic",
  num_partitions: 6,
  replication_factor: 3,
  timeout: 30_000
)

# Create a topic with custom config
{:ok, result} = KafkaEx.New.KafkaExAPI.create_topic(client, "compacted-topic",
  num_partitions: 3,
  replication_factor: 2,
  config_entries: [
    {"cleanup.policy", "compact"},
    {"retention.ms", "86400000"}
  ]
)

# Create multiple topics at once
topics = [
  [topic: "topic1", num_partitions: 3],
  [topic: "topic2", num_partitions: 6, replication_factor: 2],
  [topic: "topic3", num_partitions: 1, config_entries: [{"retention.ms", "3600000"}]]
]
{:ok, result} = KafkaEx.New.KafkaExAPI.create_topics(client, topics, 30_000)

# Validate without creating (dry-run)
{:ok, result} = KafkaEx.New.KafkaExAPI.create_topics(client, topics, 10_000,
  validate_only: true,
  api_version: 1
)

# Access results
CreateTopics.success?(result)           # Check if all succeeded
CreateTopics.failed_topics(result)      # Get failed topics
CreateTopics.successful_topics(result)  # Get successful topics
CreateTopics.get_topic_result(result, "my-topic")  # Get specific result

# Check individual topic result
topic_result = CreateTopics.get_topic_result(result, "my-topic")
topic_result.topic         # Topic name
topic_result.error         # :no_error or error atom
topic_result.error_message # Error message (V1+, nil for V0)
TopicResult.success?(topic_result)  # Check if this topic succeeded
```

**Migration Checklist:**
- [x] Phase 1: Data Structures (CreateTopics + TopicResult structs, 17 tests)
- [x] Phase 2: Protocol Implementation (V0, V1, V2 request/response, 37 tests)
- [x] Phase 3: Infrastructure Integration (RequestBuilder, ResponseParser, KayrockProtocol, Client)
- [x] Phase 4: Backward Compatibility (Legacy API via ClientCompatibility)
- [x] Phase 5: Public API (create_topics/4, create_topic/3)
- [x] Phase 6: Unit Tests (54 tests passing)
- [x] Phase 7: Integration Tests (11 tests passing)
- [x] Phase 8: Code Quality (format, credo passing)

---
