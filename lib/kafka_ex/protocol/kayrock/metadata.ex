defmodule KafkaEx.Protocol.Kayrock.Metadata do
  @moduledoc """
  This module handles Metadata request & response parsing.

  The Metadata API provides information about the Kafka cluster including:
  - Available brokers (node_id, host, port, rack)
  - Topics and their partition metadata
  - Partition leaders and replicas
  - Controller broker identification (V1+)

  Request is built using Kayrock protocol, response is parsed to
  native KafkaEx structs (ClusterMetadata).

  ## Supported Versions

  - **V0**: Basic metadata (brokers, topics, partitions)
  - **V1**: Adds controller_id, is_internal flag, broker rack
  - **V2**: Adds cluster_id
  - **V3**: Adds throttle_time_ms in response
  - **V4**: Adds allow_auto_topic_creation in request
  - **V5**: Adds offline_replicas in partition metadata
  - **V6**: Same schema as V5
  - **V7**: Adds leader_epoch in partition metadata
  - **V8**: Adds include_cluster/topic_authorized_operations in request,
    cluster/topic_authorized_operations in response
  - **V9**: Flexible version (KIP-482) with compact encodings + tagged_fields

  All versions V0-V9 have explicit protocol implementations. The `Any`
  fallback is retained only for forward compatibility with unknown future
  versions.

  ## Usage

  Requests are built by implementing the `Request` protocol for each Kayrock
  version struct. Responses are parsed by implementing the `Response` protocol.

  ```elixir
  # Build a V1 request
  request = %Kayrock.Metadata.V1.Request{}
  built_request = Metadata.Request.build_request(request, topics: ["topic1", "topic2"])

  # Parse a V1 response
  {:ok, cluster_metadata} = Metadata.Response.parse_response(kayrock_response)
  ```
  """

  alias KafkaEx.Cluster.ClusterMetadata

  defprotocol Request do
    @moduledoc """
    This protocol is used to build Metadata requests.

    Each Kayrock.Metadata version (V0-V9) implements this protocol
    to transform request options into the appropriate Kayrock request struct.

    The `Any` fallback is retained only for forward compatibility with
    unknown future versions beyond what Kayrock currently supports.
    """

    @fallback_to_any true

    @doc """
    Builds a Metadata request from options.

    ## Options

    - `:topics` - List of topic names to fetch metadata for, or `nil`/`[]` for all topics
    - `:allow_auto_topic_creation` - Whether to auto-create topics (V4+)

    ## Returns

    A Kayrock.Metadata request struct ready to be sent to Kafka.
    """
    @spec build_request(t(), Keyword.t()) :: t()
    def build_request(request, opts)
  end

  defprotocol Response do
    @moduledoc """
    This protocol is used to parse Metadata responses.

    Each Kayrock.Metadata version (V0-V9) implements this protocol
    to transform the Kayrock response into a ClusterMetadata struct.

    The `Any` fallback is retained only for forward compatibility with
    unknown future versions beyond what Kayrock currently supports.
    """

    @fallback_to_any true

    @doc """
    Parses a Metadata response into ClusterMetadata.

    ## Returns

    - `{:ok, cluster_metadata}` - Successfully parsed metadata
    - `{:error, reason}` - Parse error or Kafka error
    """
    @spec parse_response(t()) :: {:ok, ClusterMetadata.t()} | {:error, term}
    def parse_response(response)
  end
end
