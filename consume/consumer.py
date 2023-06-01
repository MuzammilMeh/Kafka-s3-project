# main.py (Consumer)

import boto3
from fastapi import FastAPI
from confluent_kafka import Consumer, KafkaError

app = FastAPI()

kafka_bootstrap_servers = (
    "broker:29092"  # Update to use the service name of the Kafka broker container
)


@app.get("/")
def consume_data():
    # Create Kafka consumer
    kafka_consumer = Consumer({
        'bootstrap.servers': kafka_bootstrap_servers,
        'group.id': 'my-consumer-group',
        'auto.offset.reset': 'earliest'
    })

    kafka_consumer.subscribe(['my-new'])

    # Consume messages
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

    kafka_consumer.close()

    return {"message": "Data consumed from Kafka"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)

