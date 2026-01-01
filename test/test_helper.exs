ExUnit.start()
{:ok, _} = Application.ensure_all_started(:hammox)

ExUnit.configure(
  timeout: 120 * 1000,
  exclude: [
    integration: true,
    sasl: true
  ]
)
