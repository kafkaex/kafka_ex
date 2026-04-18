defmodule KafkaEx.Support.OptionalDeps do
  @moduledoc """
  Startup-time validation for optional-dependency modules.

  A handful of kafka_ex features (SASL mechanisms, compression
  algorithms) rely on libraries marked `optional: true` in `mix.exs`
  so apps that don't need them don't pay the install cost. The
  downside is that a misconfigured app compiles and boots, only to
  `UndefinedFunctionError` on first produce or first auth.

  This module crosses that surface into startup: given the user's
  runtime configuration, ensure the backing modules are loadable
  and raise with a concrete mix.exs snippet otherwise.
  """

  alias KafkaEx.Auth.Config, as: AuthConfig

  @doc """
  Validates that all optional-dep modules the configuration implies
  are actually loaded. Raises `ArgumentError` with a mix.exs
  migration snippet on the first missing dep; otherwise returns `:ok`.

  The `auth` argument should be the resolved `%AuthConfig{}` (or nil)
  that the Client will pass to NetworkClient. Compression checks are
  driven by the `:required_compression` application-env key — setting
  it is optional; it exists so apps that rely on compression can
  fail at boot instead of at first produce.

      # mix.exs has :snappyer in deps
      config :kafka_ex,
        required_compression: [:snappy]
  """
  @spec validate!(AuthConfig.t() | nil) :: :ok
  def validate!(auth) do
    validate_sasl!(auth)
    validate_compression!(Application.get_env(:kafka_ex, :required_compression, []))
    :ok
  end

  # --- SASL ---

  defp validate_sasl!(nil), do: :ok

  defp validate_sasl!(%AuthConfig{mechanism: :msk_iam}) do
    ensure_modules!(
      [:aws_signature, :aws_credentials, Jason],
      :msk_iam,
      """
      MSK IAM SASL requires the following optional dependencies. Add to
      your application's mix.exs:

          {:aws_signature, "~> 0.4.2"},
          {:aws_credentials, "~> 1.0"},
          {:jason, "~> 1.0"}
      """
    )
  end

  defp validate_sasl!(%AuthConfig{}), do: :ok

  # --- Compression ---

  defp validate_compression!([]), do: :ok

  defp validate_compression!(list) when is_list(list) do
    Enum.each(list, &validate_one_compression!/1)
  end

  defp validate_compression!(other) do
    raise ArgumentError,
          "config :kafka_ex, required_compression: #{inspect(other)} — expected a list " <>
            "of atoms (any of :gzip, :snappy, :lz4, :zstd). `:gzip` needs no extra dep."
  end

  defp validate_one_compression!(:none), do: :ok
  defp validate_one_compression!(:gzip), do: :ok

  defp validate_one_compression!(:snappy) do
    ensure_modules!(
      [:snappyer],
      :snappy,
      """
      Snappy compression requires the optional `snappyer` dependency. Add to
      your application's mix.exs:

          {:snappyer, "~> 1.2"}
      """
    )
  end

  defp validate_one_compression!(:lz4) do
    ensure_modules!(
      [:lz4b],
      :lz4,
      """
      LZ4 compression requires the optional `lz4b` dependency. Add to
      your application's mix.exs:

          {:lz4b, "~> 0.0.13"}
      """
    )
  end

  defp validate_one_compression!(:zstd) do
    ensure_modules!(
      [:ezstd],
      :zstd,
      """
      Zstd compression requires the optional `ezstd` dependency. Add to
      your application's mix.exs:

          {:ezstd, "~> 1.0"}
      """
    )
  end

  defp validate_one_compression!(other) do
    raise ArgumentError,
          "config :kafka_ex, required_compression: [..., #{inspect(other)}, ...] — " <>
            "unknown compression. Expected any of :gzip, :snappy, :lz4, :zstd."
  end

  # --- Helpers ---

  defp ensure_modules!(modules, feature, fix_snippet) do
    missing = Enum.reject(modules, &module_loadable?/1)

    if missing != [] do
      raise ArgumentError, """
      kafka_ex was configured to use #{inspect(feature)} but the following \
      optional dependencies are not loaded:

          #{format_missing(missing)}

      #{String.trim_trailing(fix_snippet)}

      After editing mix.exs, run `mix deps.get && mix deps.compile` and
      restart. This check runs at Client.init/1 so misconfigurations
      fail at boot rather than at first produce/auth.
      """
    end
  end

  defp module_loadable?(mod) when is_atom(mod), do: Code.ensure_loaded?(mod)

  defp format_missing(modules), do: Enum.map_join(modules, ", ", &inspect/1)
end
