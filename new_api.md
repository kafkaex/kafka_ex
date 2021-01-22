# KafkaEx 1.0 API

This document describes the design approach and gives an overview of thew new
client API.  The API itself is documented in `KafkaEx.New.KafkaExAPI`.  The
current plan is for `KafkaEx.New.KafkaExAPI` to replace `KafkaEx` in the v1.0
release.

The new API is designed to continue to provide a useful Kafka client API
foremost, to address some of the limitations and inconveniences of the
existing API (both in terms of usage and maintenance).  A central goal of the
new API is to allow us to support new Kafka features more rapidly than in the
past.  

## Status

The new API is still in very early stages of development.  We will try to keep
this section up-to-date with respect to what features have been implemented.
`KafkaEx.New.KafkaExAPI` is the source of truth for this summary.

Features implemented:

*   Get latest offset for a partition as `{:ok, offset}` or `{:error, error_code}`
    (no more fishing through the response structs).
*   Get metadata for an arbitrary list of topics

## Major Differences from the Legacy API

*   There is currently no supervisor for clients.  It is assumed that the user
    will manage these when not used in a consumer group.  (This does not apply to
    clients started via the legacy `create_worker` API, which are started under the standard
    supervision tree.)
*   The client does not automatically fetch metadata for all topics as this can
    lead to timeouts on large clusters.  There should be no observable impact here
    because the client fetches metadata for specific topics on-demand.
*   A client is no longer "attached" to a specific consumer group.  In the legacy
    implementation this was a consequence of the way autocommit was handled.

## Design Philosophy

Two main design principles in the new client are driven by factors that made
maintenance of the legacy API difficult:

1.  Delegate and generalize API message version handling

    Kafka API message serialization and deserialization has been externalized to
    a library ([Kayrock](https://github.com/dantswain/kayrock)) that can easily
    support new message formats as they are released. Kayrock exposes a generic
    serialize/deserialize functionality, which means we do not have to write code
    to handle specific versions of specific messages at a low level in KafkaEx.


2.  Separation of connection state management and API logic

    As much as possible, we avoid putting API logic inside the client GenServer.
    Instead, we write functions that form Kayrock request structs based on user
    input, use `KafkaEx.New.Client.send_request/3` to perform communication with
    the cluster, and then act accordingly based on the response.

### Example: Fetching Kafka Config Values

Suppose that we wanted to implement a function to retrieve Kafka broker
config settings. This is the DescribeConfig API and corresponds to the
`Kayrock.DescribeConfig` namespace. After some research (and trial-and-error
using iex and functions like the ones below), we find that to fetch broker
settings, we find that we need to set `resource_type` to 4 and
`resource_name` to the string representation of the broker's id number (e.g.,
`1 => "1"`):

```
# List all config values for node 1 of the cluster
{:ok, pid} = KafkaEx.start_link_worker(:no_name, server_impl: KafkaEx.New.Client)

# resource type 4 is 'broker'
req = %Kayrock.DescribeConfigs.V0.Request{
    resources: [%{resource_type: 4, resource_name: "1", config_names: nil}]
}

KafkaEx.New.Client.send_request(pid, req, KafkaEx.New.NodeSelector.node_id(1))  

{:ok,
 %Kayrock.DescribeConfigs.V0.Response{
   correlation_id: 15,
   resources: [
     %{
       config_entries: [
         %{
           config_name: "advertised.host.name",
           config_value: nil,
           is_default: 1,
           is_sensitive: 0,
           read_only: 1
         },
         %{
           config_name: "log.cleaner.min.compaction.lag.ms",
           config_value: "0",
           is_default: 1,
           is_sensitive: 0,
           read_only: 1
         },

         ...

       ]
     }
   ]
 }}
```

From the above, we could feasibly write a convenience function to fetch broker
configs and doing so would not require us to modify the client code at all.
Furthermore, as the DescribeConfigs API changes over time, we can easily support
it by changing only that function.  The goal of the new API is that most simple
functionality can be implemented simply by writing such a function:

```
alias KafkaEx.New.Client
alias KafkaEx.New.NodeSelector

def get_broker_config_values(client, config_names, broker_id) do
  request = %Kayrock.DescribeConfigs.V0.Request{
      resources: [%{
          resource_type: 4,
          resource_name: "#{broker_id}",
          config_names: config_names
      }]
  }

  # note this requires no changes to the client itself
  {:ok, %{
      resources: [
          %{
              error_code: 0,
              config_entries: config_entries
          }
      ]
  }} = Client.send_request(client, request, NodeSelector.node_id(broker_id))

  config_entries
end
```

This implementation only supports version 0 of the DescribeConfigs API.  We can
achieve some level of forward compatibility by adding an `opts` keyword list
and some code to handle `api_version` in the opts:

```
def get_broker_config_values(client, config_names, broker_id, opts \\ []) do
  api_version = Keywork.get(opts, :api_version, 0)
  # a setting in v1+
  include_synonyms = Keyword.get(opts, :include_synonyms, false)

  request = Kayrock.DescribeConfigs.get_request_struct(api_version)

  request = 
    if api_version > 1 do
      %{request | include_synonyms: include_synonyms}
    else
      request
    end

  # rest is the same
end
```