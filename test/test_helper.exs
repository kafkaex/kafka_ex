ExUnit.start()

ExUnit.configure exclude: [integration: true]

defmodule TestHelper do
  def get_bootstrap_hosts do
    Mix.Config.read!("config/config.exs") |> hd |> elem(1) |> hd |> elem(1)
  end

  def generate_random_string(string_length \\ 20) do
    :random.seed(:os.timestamp)
    Enum.map(1..string_length, fn _ -> (:random.uniform * 25 + 65) |> round end) |> to_string
  end

  def wait_until_topic_available(metadata, client, topic) do
    {metadata, client} = KafkaEx.Metadata.update(metadata, client, true)
    if metadata.topics[topic].error_code == :leader_not_available do
      :timer.sleep(500)
      wait_until_topic_available(metadata, client, topic)
    else
      {metadata, client}
    end
  end
end
