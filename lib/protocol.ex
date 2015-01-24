defmodule Kafka.Protocol do
  @produce_request   0
  @fetch_request     1
  @offset_request    2
  @metadata_request  3

  @api_version  0

  defp api_key(:produce) do
    @produce_request
  end

  defp api_key(:fetch) do
    @fetch_request
  end

  defp api_key(:offset) do
    @offset_request
  end

  defp api_key(:metadata) do
    @metadata_request
  end

  def create_request(type, connection) do
    << api_key(type) :: 16, @api_version :: 16, connection.correlation_id :: 32,
       byte_size(connection.client_id) :: 16, connection.client_id :: binary >>
  end
end
