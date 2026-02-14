# SASL Authentication

KafkaEx supports SASL authentication for secure Kafka clusters. Multiple mechanisms are available with flexible configuration options.

## Supported Mechanisms

- **PLAIN** - Simple username/password (requires SSL/TLS)
- **SCRAM-SHA-256** - Secure challenge-response authentication (Kafka 0.10.2+)
- **SCRAM-SHA-512** - Secure challenge-response with stronger hash (Kafka 0.10.2+)
- **OAUTHBEARER**   - Token-based authentication using OAuth 2.0 bearer tokens (Kafka 2.0+)
- **MSK_IAM**       - AWS IAM authentication for Amazon MSK (MSK 2.7.1+)

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
# 9392 - SASL/Oauthbearer
```

## Security Considerations

- Always use SSL/TLS with PLAIN mechanism - plain text passwords must be encrypted in transit
- Use environment variables for credentials - never hardcode passwords
- SCRAM is preferred over PLAIN when both are available

### Minimum Kafka Versions

- PLAIN: Kafka 0.9.0+
- SCRAM: Kafka 0.10.2+
- SASL-Oauthbearer: Kafka 2.0+

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

 ##  OAUTHBEARER
 ```elixir
 config :kafka_ex,
   brokers: [{"localhost", 9394}],
   use_ssl: true,
   sasl: %{
     mechanism: :oauthbearer,
     mechanism_opts: %{
       token_provider: &MyOAuth.get_token/0,
       extensions: %{"traceId" => "optional"}
     }
   }
 ```

## AWS MSK IAM (msk_iam)

AWS MSK IAM authentication for Amazon MSK clusters using IAM credentials.

```elixir
config :kafka_ex,
  brokers: [{"b-1.mycluster.kafka.us-east-1.amazonaws.com", 9098}],
  use_ssl: true,
  sasl: %{
    mechanism: :msk_iam,
    mechanism_opts: %{
      region: "us-east-1"
    }
  }
```

### Credentials Resolution

Credentials are resolved in order:

1. `credential_provider` - custom 0-arity function returning `{:ok, access_key, secret_key}` or `{:ok, access_key, secret_key, session_token}`
2. `access_key_id` + `secret_access_key` - explicit credentials in config
3. `aws_credentials` - automatic discovery supporting:
   - Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`)
   - IRSA (IAM Roles for Service Accounts on EKS)
   - Instance metadata (EC2, ECS task roles)
   - Credential files (`~/.aws/credentials`)
   - Web Identity Token (`AWS_WEB_IDENTITY_TOKEN_FILE`)

### Explicit Credentials

```elixir
mechanism_opts: %{
  region: "us-east-1",
  access_key_id: System.get_env("AWS_ACCESS_KEY_ID"),
  secret_access_key: System.get_env("AWS_SECRET_ACCESS_KEY"),
  session_token: System.get_env("AWS_SESSION_TOKEN")
}
```

### Custom Credentials Provider

```elixir
mechanism_opts: %{
  region: "us-east-1",
  credential_provider: fn ->
    {:ok, "AKIA...", "secret...", "session..."}
  end
}
```

### Dependencies

Add to `mix.exs`:

```elixir
{:aws_signature, "~> 0.4"},   # SigV4 signing
{:aws_credentials, "~> 1.0"}  # credential discovery (IRSA, instance metadata, etc.)
```

`aws_credentials` is optional if you provide explicit credentials or a custom `credential_provider`.

### IAM Policy

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["kafka-cluster:Connect", "kafka-cluster:DescribeCluster"],
      "Resource": "arn:aws:kafka:us-east-1:123456789:cluster/my-cluster/*"
    },
    {
      "Effect": "Allow",
      "Action": ["kafka-cluster:*Topic*", "kafka-cluster:ReadData", "kafka-cluster:WriteData"],
      "Resource": "arn:aws:kafka:us-east-1:123456789:topic/my-cluster/*"
    },
    {
      "Effect": "Allow",
      "Action": ["kafka-cluster:AlterGroup", "kafka-cluster:DescribeGroup"],
      "Resource": "arn:aws:kafka:us-east-1:123456789:group/my-cluster/*"
    }
  ]
}
```

### Notes

- Port **9098** (not 9092/9094)
- Requires MSK with Kafka 2.7.1+
- Host is injected automatically from connection context for SigV4 signing

## Integration with Existing Code

SASL authentication is transparent to the rest of your KafkaEx usage:

```elixir
# Once configured, use KafkaEx normally
{:ok, client} = KafkaEx.API.start_client()
{:ok, metadata} = KafkaEx.API.metadata(client)
{:ok, _} = KafkaEx.API.produce_one(client, "my-topic", 0, "message")
{:ok, result} = KafkaEx.API.fetch(client, "my-topic", 0, 0)
```

## Troubleshooting

- **Connection refused**: Ensure you're using the correct port for SASL (9192 for PLAIN, 9292 for SCRAM in test setup)
- **Authentication failed**: Check credentials and ensure the user exists in Kafka with the correct SASL mechanism configured
- **SSL handshake error**: Verify SSL certificates or use verify: :verify_none for testing (not production!)
- **Unsupported mechanism**: Ensure your Kafka version supports the mechanism (SCRAM requires 0.10.2+)
- **MSK connection timeout**: Ensure security groups allow port 9098
- **MSK auth failed**: Verify IAM policy includes `kafka-cluster:Connect`

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

- Kafka 0.10.0-0.10.1: Queries API versions, supports PLAIN only
- Kafka 0.10.2+: Full support including SCRAM mechanisms
- Kafka 2.0+: OAUTHBEARER support
- MSK 2.7.1+: MSK_IAM support

### Technical Details

- Authentication occurs immediately after socket creation
- The implementation handles packet mode switching between raw and length-prefixed formats
- Correlation IDs are used to match requests with responses
- Server signatures are validated in SCRAM authentication
- Passwords are never logged and are redacted in inspect output
