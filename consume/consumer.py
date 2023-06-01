from confluent_kafka import Consumer, KafkaError

kafka_bootstrap_servers = "broker:29092"  # Update to use the service name of the Kafka broker container


def consume_data():
    # Create Kafka consumer
    kafka_consumer = Consumer({
        'bootstrap.servers': kafka_bootstrap_servers,
        'group.id': 'my-consumer-group',
        'auto.offset.reset': 'earliest',
        'enable.partition.eof': 'false'  # Add this property to disable EOF (end-of-file) event for partitions
    })

    kafka_consumer.subscribe(['my-topic'])

    # Consume messages
    try:
        while True:
            message = kafka_consumer.poll(1.0)

            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print('Error: {}'.format(message.error()))
                    break

            # Process the consumed message
            print('Received message: {}'.format(message.value().decode('utf-8')))
    except KeyboardInterrupt:
        pass
    finally:
        kafka_consumer.close()


if __name__ == "__main__":
    consume_data()

