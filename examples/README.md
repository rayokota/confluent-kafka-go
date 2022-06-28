
Examples:

  consumer_channel_example - Channel based consumer
  consumer_example - Function & callback based consumer
  consumer_offset_metadata - Commit offset with metadata

  producer_channel_example - Channel based producer
  producer_example - Function based producer

  transactions_example - Showcasing a transactional consume-process-produce application

  avro_generic_producer_example - producer with Schema Registry and Avro Generic Serializer

  avro_generic_consumer_example - consumer with Schema Registry and Avro Generic Deserializer
  
  avro_specific_producer_example - producer with Schema Registry and Avro Specific Serializer
  
  avro_specific_consumer_example - consumer with Schema Registry and Avro Specific Deserializer
  
  protobuf_producer_example - producer with Schema Registry and Protocol Buffers Serializer
  
  protobuf_consumer_example - consumer with Schema Registry and Protocol Buffers Deserializer
  
  json_producer_example - producer with Schema Registry and JSON Schema Serializer
  
  json_consumer_example - consumer with Schema Registry and JSON Schema Deserializer

  go-kafkacat - Channel based kafkacat Go clone

  oauthbearer_example - Provides unsecured SASL/OAUTHBEARER example


Usage example:

    $ cd consumer_example
    $ go build   (or 'go install')
    $ ./consumer_example    # see usage
    $ ./consumer_example mybroker mygroup mytopic

