# SASL Authentication

KafkaEx supports SASL authentication for secure Kafka clusters. Multiple mechanisms are available with flexible configuration options.

## Supported Mechanisms

- **PLAIN** - Simple username/password (requires SSL/TLS)
- **SCRAM-SHA-256** - Secure challenge-response authentication (Kafka 0.10.2+)
- **SCRAM-SHA-512** - Secure challenge-response with stronger hash (Kafka 0.10.2+)

## Configuration

### Via Application Config

```elixir
# config/config.exs
config :kafka_ex,
  brokers: [{"localhost", 9292}],
  use_ssl: true,
  ssl_options: [
    verify: :verify_peer,
    cacertfile: "/path/to/ca-cert"
  ],
  sasl: %{
    mechanism: :scram,
    username: System.get_env("KAFKA_USERNAME"),
    password: System.get_env("KAFKA_PASSWORD"),
    mechanism_opts: %{algo: :sha256}  # :sha256 or :sha512
  }
```

### Via Worker Options

```elixir
opts = [
  uris: [{"broker1", 9092}, {"broker2", 9092}],
  use_ssl: true,
  ssl_options: [verify: :verify_none],
  auth: KafkaEx.Auth.Config.new(%{
    mechanism: :plain,
    username: "alice",
    password: "secret123"
  })
]

{:ok, pid} = KafkaEx.create_worker(:my_worker, opts)
```

### Docker Compose Setup

The project includes Docker configurations for testing SASL authentication:

```bash
# Start Kafka with SASL enabled
docker-compose up -d

# Ports:
# 9092 - No authentication (SSL)
# 9192 - SASL/PLAIN (SSL)
# 9292 - SASL/SCRAM (SSL)
```

## Security Considerations

- Always use SSL/TLS with PLAIN mechanism - plain text passwords must be encrypted in transit
- Use environment variables for credentials - never hardcode passwords
- SCRAM is preferred over PLAIN when both are available

### Minimum Kafka Versions

- PLAIN: Kafka 0.9.0+
- SCRAM: Kafka 0.10.2+

## Testing with Different Mechanisms

```elixir
# Test PLAIN authentication
config :kafka_ex,
  brokers: [{"localhost", 9192}],
  use_ssl: true,
  ssl_options: [verify: :verify_none],
  sasl: %{mechanism: :plain, username: "test", password: "secret"}

# Test SCRAM-SHA-256
config :kafka_ex,
  brokers: [{"localhost", 9292}],
  use_ssl: true,
  ssl_options: [verify: :verify_none],
  sasl: %{
    mechanism: :scram,
    username: "test",
    password: "secret",
    mechanism_opts: %{algo: :sha256}
  }
```

## Integration with Existing Code

SASL authentication is transparent to the rest of your KafkaEx usage:

```elixir
# Once configured, use KafkaEx normally
KafkaEx.metadata()
KafkaEx.produce("my-topic", 0, "message")
messages = KafkaEx.fetch("my-topic", 0, offset: 0)
```

## Troubleshooting

- **Connection refused**: Ensure you're using the correct port for SASL (9192 for PLAIN, 9292 for SCRAM in test setup)
- **Authentication failed**: Check credentials and ensure the user exists in Kafka with the correct SASL mechanism configured
- **SSL handshake error**: Verify SSL certificates or use verify: :verify_none for testing (not production!)
- **Unsupported mechanism**: Ensure your Kafka version supports the mechanism (SCRAM requires 0.10.2+)

## Advanced: Custom Authentication

For OAuth or custom mechanisms, implement the `KafkaEx.Auth.Mechanism` behaviour:

```elixir
defmodule MyAuth do
  @behaviour KafkaEx.Auth.Mechanism

  def mechanism_name(_), do: "OAUTHBEARER"

  def authenticate(config, send_fun) do
    # Custom authentication logic
    :ok
  end
end
```

## Implementation Notes

### Version Compatibility

The SASL implementation handles different Kafka versions appropriately:

- Kafka 0.9.x: Skips API versions call (not supported)
- Kafka 0.10.0-0.10.1: Queries API versions, supports PLAIN only
- Kafka 0.10.2+: Full support including SCRAM mechanisms

### Technical Details

- Authentication occurs immediately after socket creation
- The implementation handles packet mode switching between raw and length-prefixed formats
- Correlation IDs are used to match requests with responses
- Server signatures are validated in SCRAM authentication
- Passwords are never logged and are redacted in inspect output
