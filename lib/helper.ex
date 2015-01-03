defmodule Kafka.Helper do
  def get_timestamp do
    {mega, secs, _} = :os.timestamp
    mega * 1000000 + secs
  end
end
