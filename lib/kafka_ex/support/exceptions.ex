defmodule KafkaEx.ConsumerGroupRequiredError do
  defexception [:message]

  def exception(%{__struct__: struct}) do
    message = "KafkaEx requests of type #{struct} require that the worker is configured for a consumer group."
    %__MODULE__{message: message}
  end

  def exception(action) when is_binary(action) do
    message = "KafkaEx #{action} requires that the worker is configured for a consumer group."
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

defmodule KafkaEx.JoinGroupError do
  @moduledoc """
  Raised when joining a consumer group fails with an unrecoverable error.
  """
  defexception [:message, :group_name, :reason]

  def exception(opts) do
    group_name = Keyword.fetch!(opts, :group_name)
    reason = Keyword.fetch!(opts, :reason)
    message = "Error joining consumer group #{group_name}: #{inspect(reason)}"
    %__MODULE__{message: message, group_name: group_name, reason: reason}
  end
end

defmodule KafkaEx.JoinGroupRetriesExhaustedError do
  @moduledoc """
  Raised when joining a consumer group fails after exhausting all retry attempts.

  This indicates repeated transient failures (coordinator unavailable, timeouts, etc.)
  that didn't resolve within the retry window.
  """
  defexception [:message, :group_name, :last_error, :attempts]

  def exception(opts) do
    group_name = Keyword.fetch!(opts, :group_name)
    last_error = Keyword.fetch!(opts, :last_error)
    attempts = Keyword.fetch!(opts, :attempts)

    message =
      "Unable to join consumer group #{group_name} after #{attempts} attempts (last error: #{inspect(last_error)})"

    %__MODULE__{message: message, group_name: group_name, last_error: last_error, attempts: attempts}
  end
end

defmodule KafkaEx.SyncGroupError do
  @moduledoc """
  Raised when syncing a consumer group fails.
  """
  defexception [:message, :group_name, :reason]

  def exception(opts) do
    group_name = Keyword.fetch!(opts, :group_name)
    reason = Keyword.fetch!(opts, :reason)
    message = "Error syncing consumer group #{group_name}: #{inspect(reason)}"
    %__MODULE__{message: message, group_name: group_name, reason: reason}
  end
end

defmodule KafkaEx.MetadataUpdateError do
  @moduledoc """
  Raised when periodic metadata updates fail after exhausting all retry attempts.

  This triggers a GenServer crash, allowing the supervisor to restart the client
  with fresh broker connections.
  """
  defexception [:message, :attempts]

  def exception(opts) do
    attempts = Keyword.fetch!(opts, :attempts)
    message = "Periodic metadata update failed after #{attempts} attempts"
    %__MODULE__{message: message, attempts: attempts}
  end
end
