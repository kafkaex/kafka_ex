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

# All brokers use SSL/TLS. Authentication mechanisms by port:
# 9092-9094 - No authentication
# 9192-9194 - SASL/PLAIN
# 9292-9294 - SASL/SCRAM
# 9392-9394 - SASL/OAUTHBEARER
```

### Test Credentials

The Docker setup includes preconfigured test users:

**PLAIN and SCRAM:**
- `test` / `secret`
- `alice` / `alice-secret`
- `admin` / `admin-secret`

**OAUTHBEARER:**
- Uses unsecured validator (testing only)
- Accept any token with subject claim

**Example test configuration:**
```elixir
# Test with PLAIN
config :kafka_ex,
  brokers: [{"localhost", 9192}],
  use_ssl: true,
  ssl_options: [verify: :verify_none],
  sasl: %{mechanism: :plain, username: "test", password: "secret"}

# Test with SCRAM-SHA-256
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

## OAUTHBEARER

```elixir
config :kafka_ex,
  brokers: [{"localhost", 9394}],
  use_ssl: true,
  sasl: %{
    mechanism: :oauthbearer,
    mechanism_opts: %{
      token_provider: &MyOAuth.get_token/0,
      extensions: %{"traceId" => "trace-123"}  # Optional KIP-342 extensions
    }
  }
```

### Token Provider Requirements

The `token_provider` must be a **0-arity function** that returns:
- `{:ok, token}` - where `token` is a non-empty binary string (typically a JWT)
- `{:error, reason}` - on failure

**Important:** The token provider is called **once per connection**, not cached by the library. Implement your own caching if needed:

```elixir
defmodule MyOAuth do
  use Agent

  def start_link(_) do
    Agent.start_link(fn -> %{token: nil, expires: 0} end, name: __MODULE__)
  end

  def get_token() do
    now = System.os_time(:second)

    case Agent.get(__MODULE__, & &1) do
      %{token: token, expires: exp} when exp > now and token != nil ->
        {:ok, token}

      _ ->
        # Fetch new token from OAuth provider
        with {:ok, token} <- fetch_from_oauth_server(),
             {:ok, expires_in} <- parse_expiry(token) do
          Agent.update(__MODULE__, fn _ ->
            %{token: token, expires: now + expires_in - 60}  # 60s buffer
          end)
          {:ok, token}
        end
    end
  end

  defp fetch_from_oauth_server(), do: # Your implementation
  defp parse_expiry(token), do: # Extract exp from JWT
end
```

### Extensions (Optional)

Extensions are KIP-342 custom key-value pairs sent to the broker:
- Must be a map of `%{string_key => string_value}`
- Cannot use reserved name `"auth"`
- Example: `%{"traceId" => "abc", "tenant" => "prod"}`

## Configuration Summary

### Quick Reference

| Mechanism | Username/Password | mechanism_opts | TLS Required | Min Kafka |
|-----------|-------------------|----------------|--------------|-----------|
| PLAIN | ✅ Required | None | ✅ Yes | 0.9.0+ |
| SCRAM | ✅ Required | `algo: :sha256\|:sha512` | ⚠️ Recommended | 0.10.2+ |
| OAUTHBEARER | ❌ Not used | `token_provider` (required), `extensions` (optional) | ⚠️ Recommended | 2.0+ |
| MSK_IAM | ❌ Not used | `region` (required), credential options | ✅ Yes | MSK 2.7.1+ |

### Detailed Configuration

**PLAIN**
- Simplest mechanism using plain username/password
- **CRITICAL:** Must use SSL/TLS (validation enforces this)
- Format: Binary concatenation per RFC 4616

**SCRAM-SHA-256 / SCRAM-SHA-512**
- Secure challenge-response authentication (RFC 5802/7677)
- Default algorithm: `:sha256`
- Password never sent in cleartext (PBKDF2 key derivation)
- Usernames with `=` or `,` are automatically escaped
- Server signature validated to prevent MITM attacks

**OAUTHBEARER**
- Uses OAuth 2.0 bearer tokens (typically JWT)
- Token provider must be 0-arity function
- Library does NOT cache tokens - implement caching in provider
- Called once per connection
- Supports KIP-342 extensions for custom metadata

**MSK_IAM**
- AWS IAM authentication using SigV4 signatures
- Requires port 9098 (MSK IAM listener)
- Uses OAUTHBEARER wire protocol internally
- Credential auto-discovery via `aws_credentials` library
- Session tokens supported for temporary credentials

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
# Once configured with SASL, use KafkaEx.API normally
{:ok, client} = KafkaEx.API.start_client()

{:ok, metadata} = KafkaEx.API.metadata(client)
{:ok, result} = KafkaEx.API.produce_one(client, "my-topic", 0, "message")
{:ok, messages} = KafkaEx.API.fetch(client, "my-topic", 0, 0)
```

## Troubleshooting

### Common Issues by Mechanism

#### PLAIN
- **`:plain_requires_tls`** - PLAIN authentication attempted without SSL/TLS enabled
  - **Solution:** Set `use_ssl: true` in configuration
- **`Connection refused`** - Wrong port or broker not listening
  - **Solution:** Verify port 9192 (test setup) or your configured PLAIN port
- **`Authentication failed`** - Invalid username/password
  - **Solution:** Check credentials match JAAS config on broker PLAIN listener

#### SCRAM
- **`Authentication failed`** - Invalid credentials or algorithm mismatch
  - **Solution:** Verify username/password and ensure `:sha256` or `:sha512` matches broker
- **`Invalid server nonce`** - Server nonce doesn't contain client nonce
  - **Solution:** Check broker SCRAM implementation (rare, likely broker bug)
- **`Invalid server signature`** - Server signature verification failed
  - **Solution:** Password mismatch or MITM attack detected
- **`Unsupported SCRAM algorithm`** - Broker doesn't support requested algorithm
  - **Solution:** Try `:sha256` (more widely supported than `:sha512`)

#### OAUTHBEARER
- **Token provider returns `{:error, ...}`** - Token fetch failed
  - **Solution:** Fix token provider implementation or OAuth server issues
- **Empty token string** - Token provider returned empty string
  - **Solution:** Ensure token provider returns non-empty binary JWT
- **Authentication failed** - Broker rejected bearer token
  - **Solution:** Check token validity, expiration, and broker OAuth configuration

#### MSK_IAM
- **`:no_credentials`** - No AWS credentials found
  - **Solution:** Set `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` or configure IAM role
- **`Connection timeout`** - Cannot reach MSK broker
  - **Solution:** Check security groups allow port 9098 from client
- **`Authentication failed`** - IAM credentials invalid or insufficient permissions
  - **Solution:** Verify IAM policy includes `kafka-cluster:Connect` action
- **`Invalid region`** - Region format incorrect
  - **Solution:** Use valid AWS region string (e.g., `"us-east-1"`)
- **SigV4 signing failure** - AWS signature generation failed
  - **Solution:** Check `aws_signature` dependency and credential format

### General Errors

- **`:unsupported_sasl_mechanism`** - Broker doesn't support requested mechanism
  - **Solution:** Check broker configuration and Kafka version compatibility
- **`:illegal_sasl_state`** - Protocol state error during authentication
  - **Solution:** Rare, indicates protocol implementation issue
- **`:correlation_mismatch`** - Request/response pairing issue
  - **Solution:** Should not occur in normal operation, file bug report
- **SSL handshake error** - TLS negotiation failed
  - **Solution:** Verify SSL certificates or use `verify: :verify_none` for testing (NOT production)

### Debugging Tips

1. **Enable Debug Logging**
   ```elixir
   config :logger, level: :debug
   ```
   This shows SASL protocol messages and authentication flow.

2. **Verify Broker Configuration**
   - Check Kafka server.properties for listener configuration
   - Verify JAAS config file has user credentials for PLAIN/SCRAM
   - Check security.inter.broker.protocol setting

3. **Test with kafka-console-producer**
   ```bash
   kafka-console-producer --bootstrap-server localhost:9192 \
     --topic test \
     --producer.config client-plain.properties
   ```
   If this works, issue is in KafkaEx configuration.

4. **Verify User Exists in Kafka**
   ```bash
   # For SCRAM users
   kafka-configs --bootstrap-server localhost:9092 \
     --describe --entity-type users --entity-name alice
   ```

5. **Check MSK IAM Policy**
   - Ensure `kafka-cluster:Connect` is allowed
   - Verify cluster ARN matches your MSK cluster
   - Check IAM role/user has policy attached

### Implementation Details

**Authentication happens during socket creation:**
- Blocking operation during `KafkaEx.API.start_client/1`
- Occurs after SSL/TLS handshake (if `use_ssl: true`)
- Failures prevent client from starting
- Re-authentication happens automatically on connection loss

**Packet mode switching:**
- Initial auth uses raw socket mode
- After successful auth, switches to length-prefixed mode (`:packet, 4`)

**Correlation IDs:**
- Used internally to match requests with responses
- Mismatches indicate protocol errors (should not happen)

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
