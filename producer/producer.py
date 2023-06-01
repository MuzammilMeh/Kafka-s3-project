# main.py

import boto3
from fastapi import FastAPI
from confluent_kafka import Producer

app = FastAPI()

kafka_bootstrap_servers = (
    "localhost:29092"  # Replace with the actual IP address of your Kafka broker
)


@app.get("/")
def get_data():
    # Retrieve data from S3
    s3 = boto3.client("s3")
    bucket_name = "muzammilmehmood-gluedata"
    key = "input_data/earnings/date=2022-05-16/earnings_1.csv"  # Update the file key based on your S3 bucket structure

    response = s3.get_object(Bucket=bucket_name, Key=key)
    data = response["Body"].read().decode("utf-8")

    # Stream data through Kafka
    kafka_producer = Producer({"bootstrap.servers": kafka_bootstrap_servers})
    kafka_topic = "my-new"

    # Send data to Kafka
    kafka_producer.produce(kafka_topic, value=data.encode("utf-8"))
    kafka_producer.flush()  # Optionally, you can call flush() to ensure all messages are sent

    return {"message": "Data sent to Kafka"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
