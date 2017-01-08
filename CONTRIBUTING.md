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

*Dialyzer note*  This repo currently produces several dialyzer warnings.  We
are working on cleaning those up.  In the meantime, try to make sure that your
contribution does not _add_ any dialyzer warnings.  You can check this by
comparing the results of `mix dialyzer` on master and your branch.
