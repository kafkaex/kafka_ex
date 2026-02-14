# Contributing

- [Fork](https://github.com/kafkaex/kafka_ex/fork), then clone the repo: `git clone git@github.com:your-username/kafka_ex.git`
- Create a feature branch: `git checkout -b feature_branch`
- Make your changes
- Make sure the tests pass with the dockerized test cluster. See the "Testing" section of the README.

### Running Tests

Unit tests (no Kafka cluster required):
```bash
mix test.unit
```

Integration tests (requires Docker test cluster):
```bash
./scripts/docker_up.sh

# Run all integration tests
mix test.integration

# Or run by category
mix test --only consumer_group
mix test --only produce
mix test --only consume
mix test --only lifecycle
mix test --only auth
```

Chaos tests (uses Testcontainers, no Docker Compose cluster needed):
```bash
mix test --only chaos
```

### Static Analysis

Make sure all checks pass before submitting:
```bash
mix format --check-formatted
mix credo --strict
mix dialyzer
```

### Submitting

- Push your feature branch: `git push origin feature_branch`
- Submit a pull request with your feature branch

Thanks!

### Requirements

KafkaEx requires Elixir 1.14+ and Erlang/OTP 24+.
