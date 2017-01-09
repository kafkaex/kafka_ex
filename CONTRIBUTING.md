# Contributing

- [Fork](https://github.com/kafkaex/kafka_ex/fork), then clone the repo: `git clone git@github.com:your-username/kafka_ex.git`
- Create a feature branch: `git checkout -b feature_branch`
- Make your changes
- Make sure the unit tests pass: `mix test --no-start`
- Make sure the integration tests pass: `mix test --only integration`
- Make sure the consumer group tests pass: `mix test --only consumer_group`
- Make sure dialyzer returns clean: `mix dialyzer` *See below*
- Push your feature branch: `git push origin feature_branch`
- Submit a pull request with your feature branch

Thanks!

*Dialyzer note* You need Elixir 1.3.2+ to run `mix dialyzer`.  You may get some
false positives on Erlang 18.  `mix dialyzer` is known to return clean on
Elixir 1.3.4 with Erlang 19.2.
