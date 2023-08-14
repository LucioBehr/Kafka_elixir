defmodule KafkaProducer do
  @client :kafka_client
  @topic "first_topic"
  @partition 0

  # KafkaProducer.start_client && KafkaProducer.start_producer &&  KafkaConsumer.start &&  KafkaProducer.send_message("a", "b")
  # KafkaProducer.send_message("a", "b") <- send_message command

  def start_producer do
    :brod.start_producer(@client, @topic, [])
    |>IO.inspect()
  end

  def start_client do
    :brod.start_client([{"localhost", 9092}], @client)
    #:brod_kafka_request.list_groups()
  end

  def send_message(key, value) do
    #{:ok, pid} = :brod_client.get(:my_client)
    :brod.produce(@client, @topic, @partition, key, value)
  end



end
