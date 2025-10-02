defmodule KafkaEx.Auth.Config do
  @moduledoc """
    Builds and validates SASL authentication config for `KafkaEx`.

    ## Responsibilities
      * Read SASL config from application env (`from_env/0`)
      * Build config from parameters (`new/1`)
      * Validate required fields for each mechanism
      * Normalize options and return a `%KafkaEx.Auth.Config{}` struct

    ## Supported mechanisms
      * `:plain` – username/password over TLS (required)
      * `:scram` – SCRAM-SHA (supports both SHA-256 and SHA-512 algorithms)

    ## Configuration

    ### Via Application Environment

    config :kafka_ex,
      sasl: %{
        mechanism: :scram,
        username: System.get_env("KAFKA_USERNAME"),
        password: System.get_env("KAFKA_PASSWORD"),
        mechanism_opts: %{algo: :sha256}  # or :sha512
      }

    ### Via Direct Options

        opts = [
        uris: [{"localhost", 9292}],
        use_ssl: true,
        ssl_options: [verify: :verify_none],
        auth: KafkaEx.Auth.Config.new(%{
          mechanism: :plain,
          username: "test",
          password: "secret"
        })
      ]

    ### Example  
    
    # Build config directly
    iex> KafkaEx.Auth.Config.new(%{mechanism: :plain, username: "u", password: "p"})
    #KafkaEx.Auth.Config<mechanism=plain, username="u", password=***REDACTED***, mechanism_opts=%{}>

    # Read from environment
    iex> Application.put_env(:kafka_ex, :sasl, %{mechanism: :scram, username: "u", password: "p"})
    iex> KafkaEx.Auth.Config.from_env()
    #KafkaEx.Auth.Config<mechanism=scram, username="u", password=***REDACTED***, mechanism_opts=%{}>

   ###  Security Notes

    * PLAIN mechanism MUST use SSL/TLS in production
    * Passwords are redacted in inspect output
    * Use environment variables for credentials, never hardcode
  """

  @enforce_keys [:mechanism, :username, :password]
  defstruct mechanism: :plain,
            username: nil,
            password: nil,
            mechanism_opts: %{}

  @type t :: %__MODULE__{
          mechanism: :plain | :scram,
          username: String.t(),
          password: String.t(),
          mechanism_opts: map()
        }

  # ---- Public constructors ----

  @spec new(map()) :: t
  def new(%{} = attrs) do
    attrs
    |> validate_config()
    |> then(fn cfg ->
      cfg
      |> Map.take([:mechanism, :username, :password, :mechanism_opts])
      |> Map.update(:mechanism_opts, %{}, &normalize_opts/1)
      |> then(&struct!(__MODULE__, &1))
    end)
  end

  @spec from_env() :: t | nil
  def from_env do
    case Application.get_env(:kafka_ex, :sasl) do
      nil ->
        nil

      raw ->
        cfg = normalize_to_map(raw)

        username = cfg[:username] || Application.get_env(:kafka_ex, :sasl_username)
        password = cfg[:password] || Application.get_env(:kafka_ex, :sasl_password)
        mech = cfg[:mechanism] || Application.get_env(:kafka_ex, :sasl_mechanism, :plain)

        if is_binary(username) and is_binary(password) do
          new(%{cfg | mechanism: mech, username: username, password: password})
        else
          nil
        end
    end
  end

  # ---- Internal helpers ----

  defp normalize_opts(nil), do: %{}
  defp normalize_opts(%{} = m), do: m
  defp normalize_opts(kw) when is_list(kw), do: Map.new(kw)

  defp normalize_to_map(v) when is_map(v), do: v
  defp normalize_to_map(v) when is_list(v), do: Map.new(v)
  defp normalize_to_map(_), do: %{}

  defp validate_config(%{mechanism: mech} = cfg) do
    case mech do
      :plain ->
        require_keys!(cfg, [:username, :password])
        cfg

      :scram ->
        require_keys!(cfg, [:username, :password])
        cfg

      _ ->
        raise ArgumentError, "Unsupported SASL mechanism: #{inspect(mech)}"
    end
  end

  defp require_keys!(map, keys) do
    missing = for k <- keys, not Map.has_key?(map, k), do: k
    if missing != [], do: raise(ArgumentError, "Missing keys: #{inspect(missing)}")
    :ok
  end

  defimpl Inspect do
    import Inspect.Algebra

    def inspect(%KafkaEx.Auth.Config{} = c, _opts) do
      concat([
        "#KafkaEx.Auth.Config<",
        "mechanism=",
        to_string(c.mechanism),
        ", ",
        "username=",
        inspect(c.username),
        ", ",
        "password=***REDACTED***, ",
        "mechanism_opts=",
        inspect(c.mechanism_opts),
        ">"
      ])
    end
  end
end
