import boto3
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

kafka_bootstrap_servers = "broker:29092"  # Update with the actual IP address of the Kafka broker


def create_topic_if_not_exists(topic, num_partitions=1, replication_factor=1):
    admin_client = AdminClient({'bootstrap.servers': kafka_bootstrap_servers})
    topic_list = []
    topic_list.append(NewTopic(topic, num_partitions=num_partitions, replication_factor=replication_factor))
    admin_client.create_topics(topic_list)


def send_data():
    # Retrieve data from S3
    s3 = boto3.client("s3")
    bucket_name = "muzammilmehmood-gluedata"
    key = "input_data/earnings/date=2022-05-16/earnings_1.csv"  # Update the file key based on your S3 bucket structure

    response = s3.get_object(Bucket=bucket_name, Key=key)
    data = response["Body"].read().decode("utf-8")

    # Create topic if not exists
    kafka_topic = "my-topic"
    create_topic_if_not_exists(kafka_topic)

    # Stream data through Kafka
    kafka_producer = Producer({"bootstrap.servers": kafka_bootstrap_servers})

    def delivery_callback(err, msg):
        if err is not None:
            print(f"Failed to deliver message: {err}")
        else:
            print(f"Message delivered to topic {msg.topic()} - partition {msg.partition()}")

    # Send data to Kafka
    kafka_producer.produce(kafka_topic, value=data.encode("utf-8"), callback=delivery_callback)
    kafka_producer.flush()


if __name__ == "__main__":
    while True:
        send_data()

