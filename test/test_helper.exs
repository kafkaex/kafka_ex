ExUnit.start()
{:ok, _} = Application.ensure_all_started(:hammox)

ExUnit.configure(
  timeout: 120 * 1000,
  exclude: [
    new_client: true,
    integration: true,
    consumer_group: true,
    server_0_p_10_p_1: true,
    server_0_p_10_and_later: true,
    server_0_p_9_p_0: true,
    server_0_p_8_p_0: true,
    sasl: true
  ]
)
