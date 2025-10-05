# Kayrock Migration Status

## Overview

This document tracks the progress of migrating KafkaEx to use Kayrock protocol implementations for all Kafka APIs. 
The migration aims to replace manual binary parsing with Kayrock's type-safe, versioned protocol layer.

**Last Updated:** 2025-10-05
**Overall Progress:** 5/20 APIs migrated (25%)

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
| **Metadata**     | 🟡 Adapter Only | v0-v2    | HIGH     | Medium     | Core infrastructure             |
| **ApiVersions**  | 🟡 Adapter Only | v0-v2    | HIGH     | Low        | Version negotiation             |
| **Produce**      | 🟡 Adapter Only | v0-v3    | HIGH     | High       | Message publishing              |
| **Fetch**        | 🟡 Adapter Only | v0-v11   | HIGH     | High       | Message consumption             |
| **ListOffsets**  | ✅ Complete     | v0-v2    | HIGH     | Medium     | Timestamp-based offset lookup   |
| **OffsetFetch**  | ✅ Complete     | v0-v3    | HIGH     | Medium     | Consumer group offset retrieval |
| **OffsetCommit** | ✅ Complete     | v0-v3    | HIGH     | Medium     | Consumer group offset storage   |

### Consumer Group Coordination APIs

| API                | Status          | Versions | Priority | Complexity   | Notes                     |
|--------------------|-----------------|----------|----------|--------------|---------------------------|
| **JoinGroup**      | 🟡 Adapter Only | v0-v5    | HIGH     | High         | Consumer group membership |
| **SyncGroup**      | 🟡 Adapter Only | v0-v3    | HIGH     | Medium       | Partition assignment      |
| **Heartbeat**      | ✅ Complete     | v0-v1    | HIGH     | Low          | Keep-alive mechanism      |
| **LeaveGroup**     | 🟡 Adapter Only | v0-v2    | MEDIUM   | Low          | Clean exit from group     |
| **DescribeGroups** | ✅ Complete     | v0-v1    | HIGH     | Medium       | Group metadata inspection |

### Topic Management APIs

| API                  | Status            | Versions | Priority | Complexity | Notes              |
|----------------------|-------------------|----------|----------|------------|--------------------|
| **CreateTopics**     | 🟡 Adapter Only   | v0-v5    | MEDIUM   | Medium     | Topic creation     |
| **DeleteTopics**     | 🟡 Adapter Only   | v0-v3    | MEDIUM   | Low        | Topic deletion     |
| **CreatePartitions** | ❌ Not Started    | v0-v1    | LOW      | Low        | Partition addition |
| **DeleteRecords**    | ❌ Not Started    | v0-v1    | LOW      | Low        | Record deletion    |

### Advanced APIs

| API                    | Status              | Versions | Priority | Complexity | Notes                    |
|------------------------|---------------------|----------|----------|------------|--------------------------|
| **DescribeConfigs**    | ❌ Not Started      | v0-v2    | LOW      | Medium     | Configuration inspection |
| **AlterConfigs**       | ❌ Not Started      | v0-v1    | LOW      | Medium     | Configuration changes    |
| **FindCoordinator**    | 🟡 Adapter Only     | v0-v2    | MEDIUM   | Low        | Coordinator discovery    |
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

### Documentation

- **Kafka Protocol Spec:** https://kafka.apache.org/protocol.html
- **Kayrock Docs:** https://hexdocs.pm/kayrock/
- **KafkaEx Docs:** https://hexdocs.pm/kafka_ex/

### Tools

- **Protocol Testing:** Use docker-compose Kafka cluster for integration tests
- **Code Quality:** `mix credo`, `mix dialyzer`, `mix format`
- **Test Runner:** `mix test --include integration`

