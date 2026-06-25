# AGENTS.md

Canonical guidance for AI coding agents (Claude Code, Cursor, Copilot, etc.)
working in this repository. `CLAUDE.md` is a symlink to this file.

For the do/don't list of public **API call patterns**, see `usage-rules.md` — read
it before writing or changing public API call sites. Dependency usage rules are
inlined under the managed marker block at the end of this file (refresh with
`mix usage_rules.sync`).

## Project

KafkaEx is an Elixir client for Apache Kafka. v1.0+ is built on [Kayrock](https://github.com/dantswain/kayrock) for protocol (de)serialization and performs **automatic API version negotiation** — there is no `kafka_version` config. Requires Elixir 1.14+, OTP 24+, Kafka 0.11.0+.

`KafkaEx.API` is the primary, modern interface; the older top-level `KafkaEx.*` worker API is retained only for backward compatibility. See `usage-rules.md` for the canonical do/don't list of API call patterns — read it before writing or changing public API call sites.

## Commands

```bash
# Unit tests — no Kafka cluster needed (excludes auth/consume/consumer_group/chaos/lifecycle/produce tags)
mix test.unit

# Integration tests — requires the Docker cluster (includes those tags)
./scripts/docker_up.sh          # start 3-broker test cluster first
mix test.integration

# Chaos / network-resilience tests (Docker + Testcontainers)
ENABLE_TESTCONTAINERS=true mix test.chaos

# Everything
mix test

# A single test file / line / tag
mix test test/kafka_ex/some_test.exs
mix test test/kafka_ex/some_test.exs:42
mix test --only consumer_group        # also: produce, consume, auth, lifecycle
mix test --include sasl               # SASL tests are excluded by default

# Static checks (all required for PRs)
mix format --check-formatted
mix credo --strict
mix dialyzer
mix compile --warnings-as-errors
```

Integration test broker ports: 9092–9094 no-auth (SSL), 9192–9194 SASL/PLAIN, 9292–9294 SASL/SCRAM, 9392–9394 SASL/OAUTHBEARER.

## Architecture

Request flow, top to bottom:

1. **`KafkaEx.API`** (`lib/kafka_ex/api.ex`) — public surface. Every function takes a `client` as the first arg. `use KafkaEx.API, client: ...` generates the same functions bound to a configured client. Returns `{:ok, result}` / `{:error, reason}`; results are native `KafkaEx.Messages.*` structs.
2. **`KafkaEx.Client`** (`lib/kafka_ex/client/client.ex`) — the `GenServer` that owns a connection to a cluster. Holds `Client.State` + `Cluster.ClusterMetadata`, selects the target broker via `Client.NodeSelector`, builds requests (`RequestBuilder`), parses responses (`ResponseParser`), and drives retries (`Support.Retry`). Network I/O goes through `KafkaEx.Network.*` (sockets, SSL).
3. **`KafkaEx.Protocol.KayrockProtocol`** (`lib/kafka_ex/protocol/kayrock_protocol.ex`) — the dispatch hub. `build_request(operation, api_version, opts)` and the parse side route by `{operation, version}` to the per-operation modules below. This module is the *only* place the rest of the client talks to the protocol layer; it's slated to become a separate package post-1.0.
4. **Per-operation protocol modules** (`lib/kafka_ex/protocol/kayrock/<operation>/`) — each operation (produce, fetch, metadata, offset_commit, join_group, …) defines two Elixir **protocols**, `Request` and `Response`, both with `@fallback_to_any true`, plus one `vN_request_impl.ex` / `vN_response_impl.ex` `defimpl` per supported Kafka API version, an `any_*_impl.ex` forward-compat fallback, and shared `request_helpers.ex` / `response_helpers.ex`. The `defimpl` is keyed on the concrete Kayrock request/response struct for that version.

**Adding support for a new API version of an operation** means adding `vN_request_impl.ex` + `vN_response_impl.ex` under that operation's directory (mirroring the existing vN files) and updating the operation's `@moduledoc` version table — not editing the dispatch hub.

### Other key areas

- **`KafkaEx.Cluster.*`** — broker/topic/partition metadata model (`ClusterMetadata`, `Broker`, `Topic`, `TopicPartition`, `PartitionInfo`).
- **`KafkaEx.Messages.*`** — the native structs returned to callers (`Fetch`, `Fetch.Record`, `RecordMetadata`, `Offset`, consumer-group descriptions, etc.). Protocol response impls produce these.
- **`KafkaEx.Consumer.*`** — `GenConsumer` behaviour (`handle_message_set/2` → `{:async_commit, state}` or `{:sync_commit, state}` — no `:no_commit`), `ConsumerGroup` coordinator with `Manager`, `Heartbeat`, and `PartitionAssignment`, plus `Stream`.
- **`KafkaEx.Auth.*`** — SASL: `plain`, `scram` (`scram_flow`), `oauthbearer`, `msk_iam`. PLAIN is enforced to require SSL (`:plain_requires_tls`).
- **`KafkaEx.Producer.*`** — partitioner and produce path; `Legacy` is the backward-compat producer.
- **`KafkaEx.Telemetry`** — 27+ events under the `[:kafka_ex, ...]` prefix.
- **`KafkaEx.Support.OptionalDeps`** — compression/auth backends (`snappyer`, `ezstd`, `lz4b`, `aws_signature`, …) are optional deps; `Client.init` validates them at boot so misconfiguration crashes loudly instead of at first use.

### API version resolution

Effective version per request = **per-request opts > application config (`:api_versions`) > broker-negotiated max**. Negotiation happens on connect via the ApiVersions request (with retry on parse errors, issue #433).

## Conventions & gotchas

- `mix compile --warnings-as-errors` is enforced — keep the build warning-free.
- The client retry budget is split across two modules: `@retry_count` in `client/client.ex` and `@fetch_max_retries` in `api.ex` must stay equal, and are kept in sync **by hand** (see #357). Change both together.
- Test mocking uses **Mimic** (recently migrated off Hammox). Test support lives in `test/support` (compiled only in `:test`).
- The protocol layer favors keeping Kafka business logic *out* of the `Client` GenServer and *in* the per-version protocol impls, so the client stays stable as the wire protocol evolves.

## Reference docs

`README.md` (full usage), `usage-rules.md` (AI agent do/don't), `AUTH.md` (all 5 SASL mechanisms), `UPGRADING.md` (0.x → 1.0 breaking changes), `kayrock.md` (protocol/version notes), `CONTRIBUTING.md` (PR checklist).

<!-- usage-rules-start -->
<!-- kayrock-start -->
## kayrock usage
_Elixir interface to the Kafka protocol_

# Kayrock Usage Rules

Kayrock is an Elixir library for Kafka protocol serialization and deserialization.
It generates Elixir structs from Kafka protocol schemas.

## Critical: The Built-in Client is NOT Production-Ready

The `Kayrock.Client` module and convenience functions like `Kayrock.produce/5` and
`Kayrock.fetch/5` are for **development and testing only**.

### DON'T use in production:

```elixir
# DON'T - Not production-ready
{:ok, client} = Kayrock.Client.start_link([{"localhost", 9092}])
Kayrock.produce(client, batch, "topic", 0)
```

### DO use Kayrock for serialization with a production client:

```elixir
# DO - Use with KafkaEx or brod
request = %Kayrock.Produce.V3.Request{
  acks: -1,
  timeout: 5000,
  topic_data: [%{topic: "my-topic", data: [...]}]
}
wire_data = Kayrock.Request.serialize(request)
# Send via KafkaEx or brod connection
```

## Struct Naming Convention

All generated structs follow this pattern:

```
Kayrock.<API>.V<version>.<Request|Response>
```

Examples:
- `Kayrock.Produce.V1.Request` - Produce API version 1 request
- `Kayrock.Fetch.V4.Response` - Fetch API version 4 response
- `Kayrock.Metadata.V1.Request` - Metadata API version 1 request

## Compression Dependencies

Compression support requires optional dependencies. Without them, you'll get runtime errors.

### Available compression formats:

| Format    | Attribute | Dependency              |
|-----------|-----------|-------------------------|
| None      | 0         | Built-in                |
| Gzip      | 1         | Built-in                |
| Snappy    | 2         | `{:snappyer, "~> 1.2"}` |
| LZ4       | 3         | `{:lz4b, "~> 0.0.13"}`  |
| Zstandard | 4         | `{:ezstd, "~> 1.0"}`    |

### Add to mix.exs if using compression:

```elixir
{:snappyer, "~> 1.2"},   # For Snappy
{:lz4b, "~> 0.0.13"},    # For LZ4
{:ezstd, "~> 1.0"},      # For Zstandard
```

## Creating Record Batches

Use `Kayrock.RecordBatch` for modern Kafka (0.11+):

```elixir
batch = %Kayrock.RecordBatch{
  attributes: 0,  # 0=none, 1=gzip, 2=snappy, 3=lz4, 4=zstd
  records: [
    %Kayrock.RecordBatch.Record{
      key: "my-key",
      value: "my-value",
      headers: [{"header-name", "header-value"}]
    }
  ]
}
```

## Serialization and Deserialization

### Serialize a request:

```elixir
request = %Kayrock.Metadata.V1.Request{
  correlation_id: 1,
  client_id: "my_app",
  topics: [%{name: "my-topic"}]
}

# Returns iodata (efficient for network)
wire_data = Kayrock.Request.serialize(request)
```

### Deserialize a response:

```elixir
# Get the deserializer for a request
deserializer = Kayrock.Request.response_deserializer(request)

# Parse binary response
{response, _rest} = deserializer.(binary_response)
```

## API Version Selection

Different Kafka versions support different API versions. Always check broker compatibility.

### Produce API versions:

| Version   | Min Kafka   | Features            |
|-----------|-------------|---------------------|
| V0-V2     | 0.9         | Basic produce       |
| V3        | 0.11        | Idempotent producer |
| V7        | 2.1         | Transaction support |

### Fetch API versions:

| Version   | Min Kafka  | Features        |
|-----------|------------|-----------------|
| V0-V3     | 0.9        | Basic fetch     |
| V4        | 0.11       | Isolation level |
| V7        | 1.1        | Session fetch   |

## Error Handling

Kafka errors are returned in response structs, not as exceptions.

```elixir
case response do
  %{error_code: 0} ->
    # Success
    :ok

  %{error_code: error_code} ->
    # Use Kayrock.ErrorCode to decode
    error_name = Kayrock.ErrorCode.code_to_atom(error_code)
    {:error, error_name}
end
```

## Common Mistakes to Avoid

1. **Don't use the built-in client in production** - Use KafkaEx or brod
2. **Don't forget compression dependencies** - They're optional, add what you need
3. **Don't hardcode API versions** - Check broker capabilities with ApiVersions
4. **Don't ignore error_code in responses** - Kafka returns errors in the response
5. **Don't assume message ordering** - Use partition keys for ordering guarantees

## Working with Multiple Partitions

```elixir
# Produce to specific partition
request = %Kayrock.Produce.V1.Request{
  topic_data: [
    %{
      topic: "my-topic",
      data: [
        %{partition: 0, record_set: batch_for_partition_0},
        %{partition: 1, record_set: batch_for_partition_1}
      ]
    }
  ]
}
```

## Quick Reference

### Check broker API versions:

```elixir
request = %Kayrock.ApiVersions.V1.Request{correlation_id: 1, client_id: "my_app"}
```

### Fetch topic metadata:

```elixir
request = %Kayrock.Metadata.V1.Request{
  correlation_id: 1,
  client_id: "my_app",
  topics: [%{name: "my-topic"}]  # or nil for all topics
}
```

### Create topics:

```elixir
request = %Kayrock.CreateTopics.V2.Request{
  correlation_id: 1,
  client_id: "my_app",
  create_topic_requests: [
    %{
      topic: "new-topic",
      num_partitions: 3,
      replication_factor: 1,
      replica_assignment: [],
      config_entries: []
    }
  ],
  timeout: 30_000
}
```

<!-- kayrock-end -->
<!-- usage-rules-end -->
