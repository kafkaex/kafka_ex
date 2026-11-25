defmodule KafkaEx.Client.RequestBuilder do
  @moduledoc """
  Builds the request `KafkaEx.Client` sends for one API, at the version chosen for it.
  """
  @protocol Application.compile_env(:kafka_ex, :protocol, KafkaEx.Protocol.KayrockProtocol)

  require Logger

  alias KafkaEx.Client.State
  alias KafkaEx.Cluster.ClusterMetadata

  # First OffsetCommit / OffsetFetch versions that identify topics by id (KIP-848).
  @offset_commit_topic_id_version 10
  # First OffsetCommit version that carries a member epoch (KIP-848).
  @offset_commit_member_epoch_version 9
  @offset_fetch_topic_id_version 10
  # First Metadata version whose brokers honour topic-id lookups.
  @metadata_topic_id_lookup_version 12
  # First ConsumerGroupHeartbeat version with subscribed_topic_regex.
  @consumer_group_heartbeat_regex_version 1
  # Kayrock encodes int32 as <<v::32-signed>>, so an out-of-range value would silently wrap.
  @int32_range -2_147_483_648..2_147_483_647

  @consumer_group_heartbeat_fields [
    :group_id,
    :member_id,
    :member_epoch,
    :instance_id,
    :rack_id,
    :rebalance_timeout_ms,
    :subscribed_topic_names,
    :subscribed_topic_regex,
    :server_assignor,
    :topic_partitions
  ]

  # The only options these APIs send; the protocol layer would read any other one it knows.
  @sent_fields %{
    api_versions: [],
    list_groups: [],
    describe_groups: [:group_names],
    list_offsets: [:topics],
    heartbeat: [:group_id, :member_id, :generation_id, :group_instance_id],
    sync_group: [:group_id, :generation_id, :member_id, :group_assignment, :group_instance_id],
    offset_fetch: [:group_id, :topics, :member_id, :member_epoch, :require_stable],
    consumer_group_heartbeat: @consumer_group_heartbeat_fields,
    consumer_group_describe: [:group_ids, :include_authorized_operations]
  }

  @doc """
  Builds the request for `api` from `opts`.

  The version is `opts[:api_version]`, else the app config's `:api_versions`, else the
  connection's negotiated max; a version above that max is `{:error, :api_version_no_supported}`.
  ApiVersions defaults to v0, since it is how a connection learns the others. OffsetCommit and
  OffsetFetch v10 name topics by id, so they are sent as v9 when an id is not in
  `state.cluster_metadata`.
  """
  @spec build(atom(), Keyword.t(), State.t()) :: {:ok, struct()} | {:error, atom()}
  def build(api, opts, state) do
    opts = if api == :api_versions, do: Keyword.put_new(opts, :api_version, 0), else: opts

    with :ok <- validate(api, opts),
         {:ok, version} <- get_api_version(state, api, opts),
         {:ok, version, fields} <- fields(api, version, opts, state) do
      {:ok, @protocol.build_request(api, version, fields)}
    end
  end

  defp validate(api, opts) when api in [:offset_fetch, :offset_commit], do: check_member_opts(opts)
  defp validate(:consumer_group_heartbeat, opts), do: validate_consumer_group_heartbeat_opts(opts)
  defp validate(_api, _opts), do: :ok

  defp fields(:offset_fetch, version, opts, state) do
    {version, topic_ids} = resolve_topic_ids(state, :offset_fetch, version, @offset_fetch_topic_id_version, opts)
    {:ok, version, opts |> Keyword.take(@sent_fields.offset_fetch) |> Keyword.put(:topic_ids, topic_ids)}
  end

  defp fields(:offset_commit, version, opts, state) do
    with :ok <- check_member_epoch_version(opts, version) do
      {version, topic_ids} = resolve_topic_ids(state, :offset_commit, version, @offset_commit_topic_id_version, opts)
      {:ok, version, Keyword.put(opts, :topic_ids, topic_ids)}
    end
  end

  defp fields(:consumer_group_heartbeat, version, opts, _state) do
    with :ok <- check_subscribed_topic_regex(version, opts) do
      {:ok, version, Keyword.take(opts, @consumer_group_heartbeat_fields)}
    end
  end

  defp fields(:metadata, version, opts, _state) do
    topics = Keyword.get(opts, :topics)
    topic_ids = Keyword.get(opts, :topic_ids) || []
    allow_auto_topic_creation = Keyword.get(opts, :allow_auto_topic_creation, false)

    with :ok <- check_topic_id_lookup(topics, topic_ids, version) do
      {:ok, version, [topics: topics, allow_auto_topic_creation: allow_auto_topic_creation, topic_ids: topic_ids]}
    end
  end

  # KIP-345: v3+ takes a members array instead of one member_id.
  defp fields(:leave_group, version, opts, _state) when version >= 3 do
    members = Keyword.get(opts, :members, [%{member_id: Keyword.fetch!(opts, :member_id)}])
    {:ok, version, [group_id: Keyword.fetch!(opts, :group_id), members: members]}
  end

  defp fields(:leave_group, version, opts, _state) do
    {:ok, version, [group_id: Keyword.fetch!(opts, :group_id), member_id: Keyword.fetch!(opts, :member_id)]}
  end

  defp fields(:fetch, version, opts, _state), do: {:ok, version, Keyword.put(opts, :api_version, version)}

  defp fields(api, version, opts, _state) when is_map_key(@sent_fields, api) do
    {:ok, version, Keyword.take(opts, Map.fetch!(@sent_fields, api))}
  end

  defp fields(_api, version, opts, _state), do: {:ok, version, opts}

  defp check_subscribed_topic_regex(api_version, request_opts)
       when api_version < @consumer_group_heartbeat_regex_version do
    if is_nil(Keyword.get(request_opts, :subscribed_topic_regex)) do
      :ok
    else
      Logger.error(
        "ConsumerGroupHeartbeat :subscribed_topic_regex needs v#{@consumer_group_heartbeat_regex_version}+, " <>
          "but v#{api_version} was selected."
      )

      {:error, :api_version_no_supported}
    end
  end

  defp check_subscribed_topic_regex(_api_version, _request_opts), do: :ok

  # Built inside the client GenServer: malformed input must be an error, not a crash that takes the client down.
  @heartbeat_field_checks [
    member_epoch: {:required, :int32},
    member_id: {:optional, :string},
    instance_id: {:optional, :string},
    rack_id: {:optional, :string},
    rebalance_timeout_ms: {:optional, :int32},
    subscribed_topic_names: {:optional, :string_list},
    subscribed_topic_regex: {:optional, :string},
    server_assignor: {:optional, :string}
  ]

  @doc """
  Validates ConsumerGroupHeartbeat options against their wire types, returning
  `:ok` or `{:error, :invalid_<field>}`. Called by the client before it opens
  the telemetry span, and again by `build/3`.
  """
  @spec validate_consumer_group_heartbeat_opts(Keyword.t()) :: :ok | {:error, atom()}
  def validate_consumer_group_heartbeat_opts(request_opts) do
    invalid =
      Enum.find(@heartbeat_field_checks, fn {field, {presence, type}} ->
        not valid_heartbeat_field?(Keyword.get(request_opts, field), presence, type)
      end)

    cond do
      invalid != nil ->
        {field, _} = invalid
        {:error, :"invalid_#{field}"}

      not valid_owned_topic_partitions?(Keyword.get(request_opts, :topic_partitions)) ->
        {:error, :invalid_topic_partitions}

      true ->
        :ok
    end
  end

  defp valid_heartbeat_field?(nil, :optional, _type), do: true
  defp valid_heartbeat_field?(nil, :required, _type), do: false
  defp valid_heartbeat_field?(value, _presence, :int32), do: is_integer(value) and value in @int32_range
  defp valid_heartbeat_field?(value, _presence, :string), do: is_binary(value)
  defp valid_heartbeat_field?(value, _presence, :string_list), do: is_list(value) and Enum.all?(value, &is_binary/1)

  defp valid_owned_topic_partitions?(nil), do: true

  defp valid_owned_topic_partitions?(topic_partitions) when is_list(topic_partitions) do
    Enum.all?(topic_partitions, fn
      %{topic_id: <<_::128>>, partitions: partitions} when is_list(partitions) ->
        Enum.all?(partitions, &(is_integer(&1) and &1 in @int32_range))

      _ ->
        false
    end)
  end

  defp valid_owned_topic_partitions?(_), do: false

  defp check_member_opts(request_opts) do
    cond do
      not valid_optional?(Keyword.get(request_opts, :member_epoch), &(is_integer(&1) and &1 in @int32_range)) ->
        {:error, :invalid_member_epoch}

      not valid_optional?(Keyword.get(request_opts, :member_id), &is_binary/1) ->
        {:error, :invalid_member_id}

      not valid_optional?(Keyword.get(request_opts, :generation_id), &(is_integer(&1) and &1 in @int32_range)) ->
        {:error, :invalid_generation_id}

      true ->
        :ok
    end
  end

  # v1-v8 would drop the epoch and send the classic generation id, which a KIP-848 broker rejects on every commit.
  defp check_member_epoch_version(request_opts, api_version) do
    if is_integer(Keyword.get(request_opts, :member_epoch)) and api_version < @offset_commit_member_epoch_version do
      Logger.error(
        "OffsetCommit with :member_epoch needs v#{@offset_commit_member_epoch_version}+, " <>
          "but v#{api_version} was selected."
      )

      {:error, :invalid_api_version}
    else
      :ok
    end
  end

  defp valid_optional?(nil, _check), do: true
  defp valid_optional?(value, check), do: check.(value)

  defp check_topic_id_lookup(topics, topic_ids, api_version) do
    cond do
      not valid_topic_ids?(topic_ids) ->
        {:error, :invalid_topic_ids}

      topic_ids == [] ->
        :ok

      # The broker ignores topic names when any topic id is present.
      topics not in [nil, []] ->
        {:error, :mixed_topic_names_and_ids}

      api_version < @metadata_topic_id_lookup_version ->
        Logger.error(
          "Metadata lookup by topic id needs Metadata v#{@metadata_topic_id_lookup_version}+, " <>
            "but v#{api_version} was selected."
        )

        {:error, :api_version_no_supported}

      true ->
        :ok
    end
  end

  defp valid_topic_ids?(topic_ids) when is_list(topic_ids), do: Enum.all?(topic_ids, &match?(<<_::128>>, &1))
  defp valid_topic_ids?(<<_::128>>), do: true
  defp valid_topic_ids?(_), do: false

  # Unknown topic ids fall back to the by-name previous version, however v10 was chosen: callers cannot supply ids.
  defp resolve_topic_ids(state, request_type, api_version, topic_id_version, request_opts)
       when api_version >= topic_id_version do
    topic_names = request_opts |> Keyword.fetch!(:topics) |> Enum.map(fn {topic, _} -> topic end)
    topic_ids = ClusterMetadata.topic_ids_by_name(state.cluster_metadata, topic_names)

    if Enum.all?(topic_names, &Map.has_key?(topic_ids, &1)) do
      {api_version, topic_ids}
    else
      Logger.debug("No topic ids known for #{inspect(topic_names)}; sending #{request_type} v#{topic_id_version - 1}.")

      {topic_id_version - 1, %{}}
    end
  end

  defp resolve_topic_ids(_state, _request_type, api_version, _topic_id_version, _request_opts) do
    {api_version, %{}}
  end

  defp get_api_version(state, request_type, request_opts) do
    case State.max_supported_api_version(state, request_type) do
      {:error, :api_not_supported_by_broker} ->
        # Bootstrap sends ApiVersions before the state is populated.
        case Keyword.fetch(request_opts, :api_version) do
          {:ok, version} ->
            {:ok, version}

          :error ->
            Logger.error("Kafka API #{request_type} is not supported by the connected broker.")
            {:error, :api_not_supported_by_broker}
        end

      {:ok, max_supported} ->
        app_config = Application.get_env(:kafka_ex, :api_versions, %{})
        app_version = Map.get(app_config, request_type)

        effective_default =
          case app_version do
            nil -> max_supported
            version -> version
          end

        requested_version = Keyword.get(request_opts, :api_version, effective_default)

        if requested_version > max_supported do
          Logger.error(
            "Requested API version #{requested_version} for #{request_type} " <>
              "exceeds max supported version #{max_supported}."
          )

          {:error, :api_version_no_supported}
        else
          {:ok, requested_version}
        end
    end
  end
end
