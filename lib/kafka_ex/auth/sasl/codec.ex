defmodule KafkaEx.Auth.SASL.Codec do
  @moduledoc """
  Behaviour defining the codec interface for SASL protocol messages.

  Implementations must handle:
    * Building request frames (API versions, handshake, authenticate)
    * Parsing response frames with correlation ID validation
    * Selecting optimal protocol versions based on broker capabilities

  This abstraction allows for different encoding strategies while
  maintaining a consistent interface for the SASL authentication flow.
  """

  @type api_versions_map :: %{integer() => {integer(), integer()}}
  @type error :: {:error, term()}

  @callback api_versions_request(corr :: integer(), version :: integer(), client_id :: binary()) :: binary()

  @callback parse_api_versions_response(data :: binary(), expected_corr :: integer()) ::
              api_versions_map() | error()

  @callback handshake_request(mechanism :: binary(), corr :: integer(), version :: integer(), client_id :: binary()) ::
              binary()

  @callback parse_handshake_response(
              data :: binary(),
              expected_corr :: integer(),
              mechanism :: binary(),
              version :: integer()
            ) ::
              :ok | error()

  @callback authenticate_request(
              auth_bytes :: binary() | nil,
              corr :: integer(),
              version :: integer(),
              client_id :: binary()
            ) ::
              binary()

  @callback parse_authenticate_response(data :: binary(), expected_corr :: integer(), version :: integer()) ::
              {:ok, binary() | nil} | error()

  @callback pick_handshake_version(api_versions :: api_versions_map() | error()) ::
              non_neg_integer()

  @callback pick_authenticate_version(api_versions :: api_versions_map() | error()) ::
              non_neg_integer()
end
