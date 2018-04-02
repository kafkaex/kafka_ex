defmodule KafkaEx.Config do
  @moduledoc """
  Configuring KafkaEx

  ```
  """ <> File.read!(Path.expand("../../config/config.exs", __DIR__)) <> """
  ```
  """

  require Logger

  @doc false
  def disable_default_worker do
    Application.get_env(:kafka_ex, :disable_default_worker, false)
  end

  @doc false
  def consumer_group do
    Application.get_env(:kafka_ex, :consumer_group, "kafka_ex")
  end

  @doc false
  def use_ssl, do: Application.get_env(:kafka_ex, :use_ssl, false)

  # use this function to get the ssl options - it verifies the options and
  #   either emits warnings or raises errors as appropriate on misconfiguration
  @doc false
  def ssl_options do
    ssl_options(use_ssl(), Application.get_env(:kafka_ex, :ssl_options, []))
  end

  @doc false
  def default_worker do
    :kafka_ex
  end

  @doc false
  def server_impl do
    :kafka_ex
      |> Application.get_env(:kafka_version, :default)
      |> server
  end

  defp server("0.8.0"), do: KafkaEx.Server0P8P0
  defp server("0.8.2"), do: KafkaEx.Server0P8P2
  defp server(_), do: KafkaEx.Server0P9P0

  # ssl_options should be an empty list by default if use_ssl is false
  defp ssl_options(false, []), do: []
  # emit a warning if use_ssl is false but options are present
  #   (this is not a fatal error and can occur if one disables ssl in the
  #    default option set)
  defp ssl_options(false, options) do
    Logger.warn("Ignoring ssl_options #{inspect options} because " <>
      "use_ssl is false.  If you do not intend to use ssl and want to " <>
      "remove this warning, set `ssl_options: []` in the KafkaEx config.")
    []
  end
  # verify that options is at least a keyword list
  defp ssl_options(true, options) do
    if Keyword.keyword?(options) do
      options
    else
      raise(
        ArgumentError,
        "SSL is enabled and invalid ssl_options were provided: " <>
          inspect(options)
      )
    end
  end
end
