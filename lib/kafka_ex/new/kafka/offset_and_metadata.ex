defmodule KafkaEx.New.Kafka.OffsetAndMetadata do
  @moduledoc """
  Container for a committed offset with optional metadata.

  This struct is used in offset commit operations to bundle an offset value
  with optional metadata that can be stored alongside it. The metadata is
  typically used for consumer group coordination or custom tracking purposes.

  Java equivalent: `org.apache.kafka.clients.consumer.OffsetAndMetadata`

  ## Fields

    * `:offset` - The committed offset value (required, must be non-negative)
    * `:metadata` - Optional metadata string (defaults to empty string)
    * `:leader_epoch` - Optional leader epoch for fencing (v3+)

  ## Examples

      # Create with offset only
      offset = OffsetAndMetadata.new(100)

      # Create with offset and metadata
      offset = OffsetAndMetadata.new(100, "consumer-v1")

      # Create using build/1
      offset = OffsetAndMetadata.build(offset: 100, metadata: "v1", leader_epoch: 5)

  """

  defstruct [:offset, metadata: "", leader_epoch: nil]

  @type t :: %__MODULE__{
          offset: non_neg_integer(),
          metadata: String.t(),
          leader_epoch: non_neg_integer() | nil
        }

  @doc """
  Creates a new OffsetAndMetadata with just an offset.

  ## Parameters

    - `offset` - The committed offset value (must be non-negative)

  ## Examples

      iex> OffsetAndMetadata.new(100)
      %OffsetAndMetadata{offset: 100, metadata: "", leader_epoch: nil}

  """
  @spec new(non_neg_integer()) :: t()
  def new(offset) when is_integer(offset) and offset >= 0 do
    %__MODULE__{offset: offset}
  end

  @doc """
  Creates a new OffsetAndMetadata with offset and metadata.

  ## Parameters

    - `offset` - The committed offset value (must be non-negative)
    - `metadata` - Metadata string to store with the offset

  ## Examples

      iex> OffsetAndMetadata.new(100, "consumer-v1")
      %OffsetAndMetadata{offset: 100, metadata: "consumer-v1", leader_epoch: nil}

  """
  @spec new(non_neg_integer(), String.t()) :: t()
  def new(offset, metadata) when is_integer(offset) and offset >= 0 and is_binary(metadata) do
    %__MODULE__{offset: offset, metadata: metadata}
  end

  @doc """
  Builds an OffsetAndMetadata struct from keyword options.

  ## Options

    - `:offset` - (required) The committed offset value
    - `:metadata` - Optional metadata string (default: "")
    - `:leader_epoch` - Optional leader epoch for fencing

  ## Examples

      iex> OffsetAndMetadata.build(offset: 100)
      %OffsetAndMetadata{offset: 100, metadata: "", leader_epoch: nil}

      iex> OffsetAndMetadata.build(offset: 100, metadata: "v1", leader_epoch: 5)
      %OffsetAndMetadata{offset: 100, metadata: "v1", leader_epoch: 5}

  """
  @spec build(Keyword.t()) :: t()
  def build(opts) do
    %__MODULE__{
      offset: Keyword.fetch!(opts, :offset),
      metadata: Keyword.get(opts, :metadata, ""),
      leader_epoch: Keyword.get(opts, :leader_epoch)
    }
  end

  @doc """
  Returns the committed offset value.

  This is provided for API compatibility with Java's `OffsetAndMetadata.offset()`.
  """
  @spec offset(t()) :: non_neg_integer()
  def offset(%__MODULE__{offset: offset}), do: offset

  @doc """
  Returns the metadata string.

  This is provided for API compatibility with Java's `OffsetAndMetadata.metadata()`.
  """
  @spec metadata(t()) :: String.t()
  def metadata(%__MODULE__{metadata: metadata}), do: metadata

  @doc """
  Returns the leader epoch if present.

  This is provided for API compatibility with Java's `OffsetAndMetadata.leaderEpoch()`.
  Returns `nil` if no leader epoch is set.
  """
  @spec leader_epoch(t()) :: non_neg_integer() | nil
  def leader_epoch(%__MODULE__{leader_epoch: epoch}), do: epoch
end
