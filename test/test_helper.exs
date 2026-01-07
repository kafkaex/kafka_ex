ExUnit.start()
{:ok, _} = Application.ensure_all_started(:hammox)

ExUnit.configure(
  timeout: 120 * 1000,
  exclude: [
    auth: true,
    consume: true,
    consumer_group: true,
    lifecycle: true,
    produce: true
  ]
)
