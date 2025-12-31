defmodule KafkaEx.ConsumerGroupRequiredError do
  defexception [:message]

  def exception(%{__struct__: struct}) do
    message =
      "KafkaEx requests of type #{struct} " <>
        "require that the worker is configured for a consumer group."

    %__MODULE__{message: message}
  end

  def exception(action) when is_binary(action) do
    message =
      "KafkaEx #{action} requires that the worker is configured " <>
        "for a consumer group."

    %__MODULE__{message: message}
  end
end

defmodule KafkaEx.InvalidConsumerGroupError do
  defexception [:message]

  def exception(consumer_group) do
    message = "Invalid consumer group: #{inspect(consumer_group)}"
    %__MODULE__{message: message}
  end
end

defmodule KafkaEx.TimestampNotSupportedError do
  defexception message: "Timestamp requires produce api_version >= 3"
end
