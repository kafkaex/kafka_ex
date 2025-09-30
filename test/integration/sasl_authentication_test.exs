defmodule KafkaEx.Integration.SaslAuthenticationTest do
  use ExUnit.Case

  @moduletag :integration

  describe "SASL/PLAIN authentication" do
    @tag sasl: :plain
    test "connects and produces/consumes with PLAIN" do
      opts = [
        uris: [{"localhost", 9192}],
        use_ssl: true,
        ssl_options: [verify: :verify_none],
        auth: KafkaEx.Auth.Config.new(%{
          mechanism: :plain,
          username: "test",
          password: "secret"
        })
      ]

      {:ok, _pid} = KafkaEx.create_worker(:plain_worker, opts)

      assert :ok =
               KafkaEx.produce("test_topic", 0, "test_message",
                 worker_name: :plain_worker
               )

      metadata = KafkaEx.metadata(worker_name: :plain_worker)
      assert %KafkaEx.Protocol.Metadata.Response{} = metadata
    end
  end

  describe "SASL/SCRAM authentication" do
    for {algo, port} <- [{:sha256, 9292}, {:sha512, 9292}] do
      @tag sasl: :scram, algo: algo
      test "connects with SCRAM-#{algo}" do
        worker_name = :"scram_#{unquote(algo)}_worker"

        opts = [
          uris: [{"localhost", unquote(port)}],
          use_ssl: true,
          ssl_options: [verify: :verify_none],
          auth: KafkaEx.Auth.Config.new(%{
            mechanism: :scram,
            username: "test",
            password: "secret",
            mechanism_opts: %{algo: unquote(algo)}
          })
        ]

        {:ok, _pid} = KafkaEx.create_worker(worker_name, opts)

        assert :ok =
                 KafkaEx.produce("test_topic", 0, "scram_test_message",
                   worker_name: worker_name
                 )

        metadata = KafkaEx.metadata(worker_name: worker_name)
        assert %KafkaEx.Protocol.Metadata.Response{} = metadata
      end
    end
  end
end
