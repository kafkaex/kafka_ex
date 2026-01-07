defmodule KafkaEx.Messages.Header do
  @moduledoc """
  Kafka message header (key-value pair).

  Headers were introduced in Kafka 0.11 (Produce API V3+) and allow attaching
  additional metadata to messages without modifying the message payload. Common
  uses include tracing information, content types, and routing metadata.

  Java equivalent: `org.apache.kafka.common.header.Header`
  """

  defstruct [:key, :value]

  @type t :: %__MODULE__{
          key: String.t(),
          value: binary()
        }

  @doc """
  Creates a new Header with the given key and value.

  ## Parameters

    - `key` - The header key (must be a string)
    - `value` - The header value (binary data)

  ## Examples

      iex> Header.new("content-type", "application/json")
      %Header{key: "content-type", value: "application/json"}

      iex> Header.new("trace-id", <<1, 2, 3>>)
      %Header{key: "trace-id", value: <<1, 2, 3>>}

  """
  @spec new(String.t(), binary()) :: t()
  def new(key, value) when is_binary(key) and is_binary(value) do
    %__MODULE__{key: key, value: value}
  end

  @doc """
  Builds a Header from keyword options.

  ## Options

    - `:key` - (required) The header key
    - `:value` - (required) The header value

  ## Examples

      iex> Header.build(key: "x-custom", value: "data")
      %Header{key: "x-custom", value: "data"}

  """
  @spec build(Keyword.t()) :: t()
  def build(opts) do
    key = Keyword.fetch!(opts, :key)
    value = Keyword.fetch!(opts, :value)
    new(key, value)
  end

  @doc """
  Creates a Header from a tuple `{key, value}`.

  This is useful for converting from the common tuple format used in many APIs.

  ## Examples

      iex> Header.from_tuple({"content-type", "text/plain"})
      %Header{key: "content-type", value: "text/plain"}

  """
  @spec from_tuple({String.t(), binary()}) :: t()
  def from_tuple({key, value}) when is_binary(key) and is_binary(value) do
    new(key, value)
  end

  @doc """
  Converts the Header to a tuple `{key, value}`.

  This is useful for interoperability with APIs that expect tuple format.

  ## Examples

      iex> header = Header.new("x-custom", "value")
      iex> Header.to_tuple(header)
      {"x-custom", "value"}

  """
  @spec to_tuple(t()) :: {String.t(), binary()}
  def to_tuple(%__MODULE__{key: key, value: value}) do
    {key, value}
  end

  @doc """
  Returns the header key.

  This is provided for API compatibility with Java's `Header.key()`.
  """
  @spec key(t()) :: String.t()
  def key(%__MODULE__{key: key}), do: key

  @doc """
  Returns the header value.

  This is provided for API compatibility with Java's `Header.value()`.
  """
  @spec value(t()) :: binary()
  def value(%__MODULE__{value: value}), do: value

  @doc """
  Converts a list of Headers to tuple format.

  ## Examples

      iex> headers = [Header.new("a", "1"), Header.new("b", "2")]
      iex> Header.list_to_tuples(headers)
      [{"a", "1"}, {"b", "2"}]

  """
  @spec list_to_tuples([t()]) :: [{String.t(), binary()}]
  def list_to_tuples(headers) when is_list(headers) do
    Enum.map(headers, &to_tuple/1)
  end

  @doc """
  Converts a list of tuples to Headers.

  ## Examples

      iex> tuples = [{"a", "1"}, {"b", "2"}]
      iex> Header.list_from_tuples(tuples)
      [%Header{key: "a", value: "1"}, %Header{key: "b", value: "2"}]

  """
  @spec list_from_tuples([{String.t(), binary()}]) :: [t()]
  def list_from_tuples(tuples) when is_list(tuples) do
    Enum.map(tuples, &from_tuple/1)
  end
end
