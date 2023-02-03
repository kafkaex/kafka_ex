defmodule KafkaEx.New.Client.RequestTemplates do
  @moduledoc """
  This module is responsible for creating request templates for Kayrock Client
  """
  alias KafkaEx.New.Client.State

  alias Kayrock.DescribeGroups
  alias Kayrock.ListOffsets

  @default_api_version %{
    describe_groups: 1,
    list_offsets: 1
  }

  @spec request_template(:describe_groups, State.t()) ::
          DescribeGroups.request_t()
  @spec request_template(:list_offsets, State.t()) ::
          ListOffsets.request_t()
  def request_template(:describe_groups, state) do
    state
    |> get_api_version(:describe_groups)
    |> DescribeGroups.get_request_struct()
  end

  def request_template(:list_offsets, state) do
    state
    |> get_api_version(:list_offsets)
    |> ListOffsets.get_request_struct()
  end

  defp get_api_version(state, request_type) do
    default = Map.fetch!(@default_api_version, request_type)
    State.max_supported_api_version(state, request_type, default)
  end
end
