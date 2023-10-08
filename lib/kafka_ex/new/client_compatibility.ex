defmodule KafkaEx.New.ClientCompatibility do
  @moduledoc false

  # this module gets injected into the new client to ensure compatibility with
  # old "KafkaEx.Server" API - any work that is done to support that
  # compatibility should be limited to this module, with the intention that this
  # module can be removed once we drop compatibility for the old API

  alias KafkaEx.New.Adapter
  alias KafkaEx.New.Structs.NodeSelector

  alias KafkaEx.New.Client.State

  alias KafkaEx.Protocol.OffsetCommit.Request, as: OffsetCommitRequest

  # it's a mixin module...
  # credo:disable-for-this-file Credo.Check.Refactor.LongQuoteBlocks

  defmacro __using__(_) do
    quote do
      ######################################################################
      # compatibility handle_call messages
      #
      # these all follow the same basic pattern of accepting a message in the format
      # that is expected from the legacy KafkaEx API, using `Adapter` to adapt that
      # request to the format matching the actual Kafka API, performing the request,
      # and then using `Adapter` to map back to the format that the legacy KafkaEx
      # API expects to return to the user
      def handle_call({:metadata, topic}, _from, state) do
        updated_state = update_metadata(state, [topic])

        {:reply, Adapter.metadata_response(updated_state.cluster_metadata), updated_state}
      end

      def handle_call({:offset, topic, partition, time}, _from, state) do
        request = Adapter.list_offsets_request(topic, partition, time)

        {response, updated_state} =
          kayrock_network_request(
            request,
            NodeSelector.topic_partition(topic, partition),
            state
          )

        adapted_response =
          case response do
            {:ok, api_response} ->
              Adapter.list_offsets_response(api_response)

            other ->
              other
          end

        {:reply, adapted_response, updated_state}
      end

      def handle_call({:produce, produce_request}, _from, state) do
        # the partitioner will need to know the topic's metadata
        #   note we also try to create the topic if it does not exist
        state = ensure_topics_metadata(state, [produce_request.topic], true)

        produce_request =
          default_partitioner().assign_partition(
            produce_request,
            Adapter.metadata_response(state.cluster_metadata)
          )

        {request, topic, partition} = Adapter.produce_request(produce_request)

        {response, updated_state} =
          kayrock_network_request(
            request,
            NodeSelector.topic_partition(topic, partition),
            state
          )

        response =
          case response do
            {:ok, :ok} -> {:ok, :ok}
            {:ok, val} -> Adapter.produce_response(val)
            _ -> response
          end

        {:reply, response, updated_state}
      end

      def handle_call({:fetch, fetch_request}, _from, state) do
        allow_auto_topic_creation = state.allow_auto_topic_creation

        true = consumer_group_if_auto_commit?(fetch_request.auto_commit, state)
        {request, topic, partition} = Adapter.fetch_request(fetch_request)

        {response, updated_state} =
          kayrock_network_request(
            request,
            NodeSelector.topic_partition(topic, partition),
            %{state | allow_auto_topic_creation: false}
          )

        {response, state_out} =
          case response do
            {:ok, resp} ->
              {adapted_resp, last_offset} = Adapter.fetch_response(resp)

              state_out =
                if last_offset && fetch_request.auto_commit do
                  consumer_group = state.consumer_group_for_auto_commit

                  commit_request = %OffsetCommitRequest{
                    topic: topic,
                    partition: partition,
                    offset: last_offset,
                    api_version: fetch_request.offset_commit_api_version
                  }

                  {commit_request, ^consumer_group} =
                    Adapter.offset_commit_request(
                      commit_request,
                      consumer_group
                    )

                  {_, updated_state} =
                    kayrock_network_request(
                      commit_request,
                      NodeSelector.consumer_group(consumer_group),
                      updated_state
                    )

                  updated_state
                else
                  updated_state
                end

              {adapted_resp, state_out}

            {:error, :no_broker} ->
              {:topic_not_found, updated_state}

            _ ->
              {response, updated_state}
          end

        {:reply, response, %{state_out | allow_auto_topic_creation: allow_auto_topic_creation}}
      end

      def handle_call({:join_group, request, network_timeout}, _from, state) do
        sync_timeout = config_sync_timeout(network_timeout)
        {request, consumer_group} = Adapter.join_group_request(request)

        {response, updated_state} =
          kayrock_network_request(
            request,
            NodeSelector.consumer_group(consumer_group),
            state,
            sync_timeout
          )

        case response do
          {:ok, resp} ->
            {:reply, Adapter.join_group_response(resp), updated_state}

          _ ->
            {:reply, response, updated_state}
        end
      end

      def handle_call({:sync_group, request, network_timeout}, _from, state) do
        sync_timeout = config_sync_timeout(network_timeout)
        {request, consumer_group} = Adapter.sync_group_request(request)

        {response, updated_state} =
          kayrock_network_request(
            request,
            NodeSelector.consumer_group(consumer_group),
            state,
            sync_timeout
          )

        case response do
          {:ok, resp} ->
            {:reply, Adapter.sync_group_response(resp), updated_state}

          _ ->
            {:reply, response, updated_state}
        end
      end

      def handle_call({:leave_group, request, network_timeout}, _from, state) do
        sync_timeout = config_sync_timeout(network_timeout)
        {request, consumer_group} = Adapter.leave_group_request(request)

        {response, updated_state} =
          kayrock_network_request(
            request,
            NodeSelector.consumer_group(consumer_group),
            state,
            sync_timeout
          )

        case response do
          {:ok, resp} ->
            {:reply, Adapter.leave_group_response(resp), updated_state}

          _ ->
            {:reply, response, updated_state}
        end
      end

      def handle_call({:heartbeat, request, network_timeout}, _from, state) do
        sync_timeout = config_sync_timeout(network_timeout)
        {request, consumer_group} = Adapter.heartbeat_request(request)

        {response, updated_state} =
          kayrock_network_request(
            request,
            NodeSelector.consumer_group(consumer_group),
            state,
            sync_timeout
          )

        case response do
          {:ok, resp} ->
            {:reply, Adapter.heartbeat_response(resp), updated_state}

          _ ->
            {:reply, response, updated_state}
        end
      end

      def handle_call({:create_topics, requests, network_timeout}, _from, state) do
        request =
          Adapter.create_topics_request(
            requests,
            config_sync_timeout(network_timeout)
          )

        {response, updated_state} =
          kayrock_network_request(request, NodeSelector.controller(), state)

        case response do
          {:ok, resp} ->
            {:reply, Adapter.create_topics_response(resp), updated_state}

          _ ->
            {:reply, response, updated_state}
        end
      end

      def handle_call({:delete_topics, topics, network_timeout}, _from, state) do
        request =
          Adapter.delete_topics_request(
            topics,
            config_sync_timeout(network_timeout)
          )

        {response, updated_state} =
          kayrock_network_request(request, NodeSelector.controller(), state)

        case response do
          {:ok, resp} ->
            {:reply, Adapter.delete_topics_response(resp),
             State.remove_topics(updated_state, topics)}

          _ ->
            {:reply, response, updated_state}
        end
      end

      def handle_call({:api_versions}, _from, state) do
        {:reply, Adapter.api_versions(state.api_versions), state}
      end

      def handle_call(:consumer_group, _from, state) do
        {:reply, state.consumer_group_for_auto_commit, state}
      end

      def handle_call({:offset_fetch, offset_fetch}, _from, state) do
        unless consumer_group?(state) do
          raise KafkaEx.ConsumerGroupRequiredError, offset_fetch
        end

        {request, consumer_group} =
          Adapter.offset_fetch_request(
            offset_fetch,
            state.consumer_group_for_auto_commit
          )

        {response, updated_state} =
          kayrock_network_request(
            request,
            NodeSelector.consumer_group(consumer_group),
            state
          )

        response =
          case response do
            {:ok, resp} -> Adapter.offset_fetch_response(resp)
            _ -> response
          end

        {:reply, response, updated_state}
      end

      def handle_call({:offset_commit, offset_commit_request}, _from, state) do
        unless consumer_group?(state) do
          raise KafkaEx.ConsumerGroupRequiredError, offset_commit_request
        end

        {request, consumer_group} =
          Adapter.offset_commit_request(
            offset_commit_request,
            state.consumer_group_for_auto_commit
          )

        {response, updated_state} =
          kayrock_network_request(
            request,
            NodeSelector.consumer_group(consumer_group),
            state
          )

        response =
          case response do
            {:ok, resp} -> Adapter.offset_commit_response(resp)
            _ -> response
          end

        {:reply, response, updated_state}
      end

      ######################################################################
      # helper functions only used for compatibility

      defp ensure_topics_metadata(state, topics, allow_topic_creation) do
        case State.topics_metadata(state, topics) do
          metadata when length(metadata) == length(topics) ->
            state

          _ ->
            {_, updated_state} = fetch_topics_metadata(state, topics, allow_topic_creation)

            updated_state
        end
      end

      ######################################################################
    end
  end
end
