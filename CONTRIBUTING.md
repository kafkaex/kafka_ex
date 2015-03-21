# Contributing

- [Fork](https://github.com/jacklund/kafka_ex/fork), then clone the repo: `git clone git@github.com:your-username/kafka_ex.git`
- Create a feature branch: `git checkout -b feature_branch`
- Make your changes
- Make sure the unit tests pass: `mix test --no-start`
- Make sure the integration tests pass: `mix test --only integration`
- Make sure dialyzer returns clean: `mix dialyze --unmatched-returns --error-handling --race-conditions --underspecs`
- Push your feature branch: `git push origin feature_branch`
- Submit a pull request with your feature branch

Thanks!
