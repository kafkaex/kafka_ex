# Contributing

Thank you for contributing to KafkaEx!

## Getting Started

- [Fork](https://github.com/kafkaex/kafka_ex/fork), then clone the repo: `git clone git@github.com:your-username/kafka_ex.git`
- Create a feature branch: `git checkout -b feature_branch`
- Make your changes
- Ensure all tests and checks pass (see below)
- Push your feature branch: `git push origin feature_branch`
- Submit a pull request with your feature branch

## Requirements

- **Elixir:** 1.14+
- **Erlang OTP:** 24+
- **Docker:** Required for integration tests

## Testing

### Unit Tests (Fastest - No Kafka Required)

Run unit tests that don't require a Kafka cluster:

```bash
mix test.unit
```

### Code Quality Checks (Required for All PRs)

Before submitting a PR, ensure all static checks pass:

```bash
# Check code formatting
mix format --check-formatted

# Run linter
mix credo --strict

# Type checking
mix dialyzer

# Strict compilation (no warnings)
mix compile --warnings-as-errors
```

To automatically format your code:

```bash
mix format
```

### Integration Tests (Requires Docker)

Integration tests require a running Kafka cluster. Start the test cluster:

```bash
./scripts/docker_up.sh
```

Run all integration tests:

```bash
mix test.integration
```

Or run specific test categories:

```bash
mix test --only consumer_group
mix test --only produce
mix test --only consume
mix test --only auth
mix test --only lifecycle
```

### SASL Authentication Tests

To run SASL/authentication tests:

```bash
mix test --include sasl
```

### Chaos Tests (Network Resilience)

Chaos tests verify behavior under network failures and require Docker + Testcontainers:

```bash
ENABLE_TESTCONTAINERS=true mix test.chaos
```

### Full Test Suite

To run the complete test suite (unit + integration):

```bash
./scripts/docker_up.sh
mix test
```

## Test Aliases

The project defines convenient test aliases:

- `mix test.unit` - Unit tests only (no Kafka required)
- `mix test.integration` - All integration tests (requires Docker)
- `mix test.chaos` - Network resilience tests (requires Docker + Testcontainers)
- `mix test` - Full test suite

## Pull Request Checklist

Before submitting your PR, verify:

- [ ] `mix test.unit` passes
- [ ] `mix format --check-formatted` passes
- [ ] `mix credo --strict` passes
- [ ] `mix dialyzer` passes
- [ ] `mix test.integration` passes (with Docker running)
- [ ] Code is documented where appropriate
- [ ] CHANGELOG.md updated for significant changes
- [ ] No deprecation warnings

## Code Style

- Follow standard Elixir formatting (`mix format`)
- Use descriptive variable and function names
- Add documentation for public functions
- Keep functions small and focused
- Add typespecs for public APIs

## Commit Messages

- Use clear, descriptive commit messages
- Reference issue numbers where applicable (#123)
- Use present tense ("Add feature" not "Added feature")

## Getting Help

- **GitHub Issues:** https://github.com/kafkaex/kafka_ex/issues
- **Slack:** #kafkaex on elixir-lang.slack.com ([request invite](http://bit.ly/slackelixir))

Thank you for your contributions!
