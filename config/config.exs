import Config

config :kafka_ex,
  # A list of brokers to connect to. This can be in either of the following formats
  #
  #  * [{"HOST", port}...]
  #  * CSV - `"HOST:PORT,HOST:PORT[,...]"`
  #  * {mod, fun, args}
  #  * &arity_zero_fun/0
  #  * fn -> ... end
  #
  # If you receive :leader_not_available
  # errors when producing messages, it may be necessary to modify "advertised.host.name" in the
  # server.properties file.
  # In the case below you would set "advertised.host.name=localhost"
  brokers: [
    {"localhost", 9092},
    {"localhost", 9093},
    {"localhost", 9094}
  ],
  #
  # OR:
  # brokers: "localhost:9092,localhost:9093,localhost:9094"
  #
  # It may be useful to configure your brokers at runtime, for example if you use
  # service discovery instead of storing your broker hostnames in a config file.
  # To do this, you can use `{mod, fun, args}` or a zero-arity function, and `KafkaEx`
  # will invoke your callback when fetching the `:brokers` configuration.
  # Note that when using this approach you must return a list of host/port pairs.
  #
  # Default consumer group for worker processes, must be a binary (string)
  # Set to :no_consumer_group to disable consumer group functionality
  default_consumer_group: "kafka_ex",
  # Partitioner for produce requests when partition is not specified
  # Options: KafkaEx.Producer.Partitioner.Default (default), KafkaEx.Producer.Partitioner.Legacy
  partitioner: KafkaEx.Producer.Partitioner.Default,
  # The client_id is the logical grouping of a set of kafka clients.
  client_id: "kafka_ex",
  # Set this value to true if you do not want the default
  # KafkaEx worker to start during application start-up -
  # i.e., if you want to start your own set of named workers
  disable_default_worker: false,
  # Timeout value, in msec, for synchronous operations (e.g., network calls).
  # Per-attempt request socket-recv timeout (ms) for synchronous requests that do
  # not derive their own (metadata, offset, heartbeat, produce ack, …). Consumer-group
  # JoinGroup/SyncGroup derive their own, longer deadlines from the group's
  # rebalance/session timeouts. Was `:sync_timeout` (now a deprecated alias, removed in 2.0).
  request_timeout: 15000,
  # Supervision max_restarts - the maximum amount of restarts allowed in a time frame
  max_restarts: 10,
  # Supervision max_seconds -  the time frame in which :max_restarts applies
  max_seconds: 60,
  # Interval in milliseconds that GenConsumer waits to commit offsets.
  commit_interval: 5_000,
  # Threshold number of messages consumed for GenConsumer to commit offsets
  # to the broker.
  commit_threshold: 100,
  # The policy for choosing a start offset when there is no valid committed
  # offset — either none exists yet (new consumer group) or an
  # :offset_out_of_range error occurs.
  # Options:
  # - `:latest` (default) - Will move the offset to the most recent.
  # - `:earliest` - Will move the offset to the oldest available.
  # - `:none` - The error will simply be raised (strict; surfaces a
  #   missing/out-of-range offset instead of silently replaying or skipping).
  auto_offset_reset: :latest,
  # Base delay (ms) for the exponential backoff between retries when a GenConsumer's
  # startup committed-offset fetch hits a retryable error (coordinator not
  # available/loading, timeout, KIP-447 unstable offset). Delays grow as
  # base * 2^attempt, capped at 5s per delay; after 6 attempts the consumer crashes
  # (supervisor restart) rather than silently resetting the offset. Default: 500.
  # load_offsets_retry_backoff_ms: 500,
  # Upper bound (ms) on the randomized jitter slept before a consumer-group
  # member rejoins after its heartbeat process dies abnormally (an OS/OOM kill,
  # an uncaught crash, or a client-call timeout). Desynchronizes a fleet hitting
  # a correlated heartbeat-kill so members don't stampede the coordinator with
  # simultaneous JoinGroups. Set to 0 to disable. Default: 1000.
  # crash_rejoin_max_jitter_ms: 1000,
  # Heartbeat-crash-loop bound. If a consumer-group member's heartbeat process
  # dies abnormally more than `crash_rejoin_max_restarts` times within
  # `crash_rejoin_window_ms`, the member stops terminally (emitting
  # `[:kafka_ex, :consumer, :member_terminated]`) and is torn down by its
  # supervisor instead of rejoining forever — so a deterministically broken
  # member fails fast and alertably rather than looping silently. Mirrors OTP's
  # max_restarts/max_seconds. To tolerate a transient kill burst, raise the
  # restart count rather than widening the window (a wider window accumulates
  # more crashes and trips more eagerly). Set restarts to `:infinity` to disable
  # the bound (loop forever) — use that rather than a 0 window (which effectively
  # disables it for any restarts > 0) or a huge restart count. (0 restarts is the
  # opposite extreme: it trips on the very first abnormal crash.) Defaults: 10
  # restarts / 60_000 ms.
  # crash_rejoin_max_restarts: 10,
  # crash_rejoin_window_ms: 60000,
  #
  # Static consumer-group membership (KIP-345). Set a STABLE, per-member-UNIQUE
  # `group_instance_id` so a member retains its partition assignment across a
  # restart instead of triggering a rebalance (the broker holds the assignment
  # until `session.timeout.ms`). Default: unset (dynamic membership).
  #
  # The value MUST be unique per member in the group — derive it per node
  # (e.g. from POD_NAME / hostname / StatefulSet ordinal). A duplicate id gets
  # the loser FENCED (`:fenced_instance_id` → terminal stop). A bare string here
  # is shared by every replica and is almost always WRONG; prefer an MFA tuple
  # or a zero-arity function that resolves a per-node value, or set it per
  # consumer group via the `:group_instance_id` start option.
  # Requires Kafka >= 2.3 and a session_timeout long enough to cover a restart.
  # group_instance_id: {MyApp.Kafka, :instance_id, []},
  #
  # API versions for Kafka protocol requests.
  # By default, KafkaEx uses the highest version supported by both the
  # connected broker and the protocol library. Use this config to pin
  # specific APIs to lower versions across your deployment.
  #
  # Resolution order: per-request opts > application config > broker-negotiated max
  #
  # api_versions: %{
  #   fetch: 5,
  #   produce: 3,
  #   metadata: 4,
  #   list_offsets: 3,
  #   offset_fetch: 3,
  #   offset_commit: 3,
  #   find_coordinator: 2,
  #   join_group: 2,
  #   heartbeat: 2,
  #   leave_group: 2,
  #   sync_group: 2,
  #   describe_groups: 3,
  #   create_topics: 3,
  #   delete_topics: 2,
  #   api_versions: 2
  # },
  # Declare compression algorithms your app relies on, so Client.init
  # validates the backing optional deps (snappyer / ezstd / lz4b) at boot
  # rather than crashing at first produce with UndefinedFunctionError.
  # Optional — leave unset if your app doesn't use compression.
  # required_compression: [:snappy],
  #
  # Delay (in ms) before retrying a broker reconnect after a socket dies.
  # Lower values reconnect faster but may hammer a down broker; higher values
  # smooth flapping brokers at the cost of longer error windows.
  sleep_for_reconnect: 400,
  # Periodic metadata refresh cadence (ms). The client issues a full Metadata
  # request this often to pick up leader elections, topic changes, and broker
  # membership changes. Lower = faster recovery from cluster changes; higher =
  # less request volume.
  metadata_update_interval: 30_000,
  # This is the flag that enables use of ssl
  use_ssl: true,
  # see SSL OPTION DESCRIPTIONS - CLIENT SIDE at http://erlang.org/doc/man/ssl.html
  # for supported options
  ssl_options: [
    # Fix warnings. More at https://github.com/erlang/otp/issues/5352
    verify: :verify_none,
    cacertfile: File.cwd!() <> "/ssl/ca-cert",
    certfile: File.cwd!() <> "/ssl/cert.pem",
    keyfile: File.cwd!() <> "/ssl/key.pem"
  ]

# SASL Authentication (optional)

# Configure SASL credentials and mechanism
# sasl: %{
#   mechanism: :scram,  # :plain or :scram
#   username: "kafka_user",         # USE ENV VARS and don't expose secrets
#   password: "kafka_password",     # USE ENV VARS and don't expose secrets
#   mechanism_opts: %{algorithm: :sha256}  # For SCRAM only, :sha256 or :sha512
# }
#
# Note: SASL/PLAIN requires SSL to be enabled for security

env_config = Path.expand("#{Mix.env()}.exs", __DIR__)

if File.exists?(env_config) do
  import_config(env_config)
end
