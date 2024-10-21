import json
import logging
import os
import time
import traceback

import boto3
import botocore.client
import botocore.exceptions
import pulsar
from rdflib import Graph, Literal, Namespace, URIRef

# create logger for annotations transformer
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.basicConfig(filename="annotations-transformer.log", encoding="utf-8", level=logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

# create formatter
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

# set the time converter to UTC
formatter.converter = time.gmtime

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)


def get_file_from_s3(s3_client: botocore.client.BaseClient, bucket: str, key: str) -> str:
    try:
        data = s3_client.get_object(Bucket=bucket, Key=key)
        return data["Body"].read().decode("utf-8")
    except botocore.exceptions.ClientError as e:
        logger.error(f"Error getting object {key} from bucket {bucket}")
        logger.error(e)
        return None


def upload_file_to_s3(s3_client: botocore.client.BaseClient, bucket: str, key: str, data: str):
    logger.info(f"Uploading annotations to {key} in bucket {bucket}")
    try:
        s3_client.put_object(Bucket=bucket, Key=key, Body=data)
    except botocore.exceptions.ClientError as e:
        logger.error(f"Error uploading object {key} to bucket {bucket}")
        logger.error(e)
        return None


def json_to_rdf(json_data: dict) -> bytes:
    g = Graph()
    ex = Namespace("http://namespace-url.org/")

    for item in json_data:
        try:
            subject = URIRef(ex[item["id"]])
        except KeyError:
            logger.error(f"Error extracting id from item {item}")
            continue
        for key, value in item.items():
            predicate = URIRef(ex[key])
            g.add((subject, predicate, Literal(value)))

    return g.serialize(format="turtle")


def get_transformed_key(key: str, source: str, target: str) -> str:
    new_key = key.replace(source, target)
    new_key = new_key.replace(".json", ".ttl")
    dest_key = f"transformed/{new_key}"
    return dest_key


def delete_key(
    s3_client: botocore.client.BaseClient, bucket_name: str, key: str, source: str, target: str
) -> list:
    dest_key = get_transformed_key(key, source, target)
    try:
        s3_client.delete_object(Bucket=bucket_name, Key=dest_key)
        return [dest_key]
    except botocore.exceptions.ClientError as e:
        logger.error(f"Error deleting object {dest_key} from bucket {bucket_name}")
        logger.error(e)


def reformat_annotations(
    s3_client: botocore.client.BaseClient, bucket_name: str, key: str, source: str, target: str
) -> list:
    logger.info(f"Reformatting annotations for key {key}")
    file = get_file_from_s3(s3_client, bucket_name, key)
    if file is None:
        return
    # Load the file as a dictionary
    annotations = json.loads(file)

    # Reformat annotations into RDF
    rdf_data = json_to_rdf([annotations])

    # Generate transformed key
    dest_key = get_transformed_key(key, source, target)

    # Upload the transformed data to S3
    upload_file_to_s3(s3_client, bucket_name, dest_key, rdf_data)

    return [dest_key]


def interpret_message(msg_json: dict, s3_client) -> tuple:

    # Extract keys
    added_keys = msg_json["added_keys"]
    updated_keys = msg_json["updated_keys"]
    deleted_keys = msg_json["deleted_keys"]

    # Extract bucket
    bucket_name = msg_json["bucket_name"]

    # Extract source and target
    source = msg_json["source"]
    target = msg_json["target"]

    for key in added_keys:
        added_keys = reformat_annotations(s3_client, bucket_name, key, source, target)
    for key in updated_keys:
        updated_keys = reformat_annotations(s3_client, bucket_name, key, source, target)
    for key in deleted_keys:
        deleted_keys = delete_key(s3_client, bucket_name, key, source, target)

    return added_keys, updated_keys, deleted_keys


def send_message(
    producer: pulsar.Producer,
    input_message: dict,
    added_keys: list,
    updated_keys: list,
    deleted_keys: list,
):
    output_message = input_message
    output_message["added_keys"] = added_keys
    output_message["updated_keys"] = updated_keys
    output_message["deleted_keys"] = deleted_keys
    message_string = json.dumps(output_message).encode("utf-8")
    producer.send(message_string)
    logger.info(f"Sent transformed message {message_string}")


def main(
    consumer: pulsar.Consumer, producer: pulsar.Producer, s3_client: botocore.client.BaseClient
):
    """
    Accept messages from a Pulsar broker.
    """
    while True:
        try:
            logger.info("Waiting for message")
            msg = consumer.receive()
            logging.info(f"Parsing harvested message {msg.data()}")
            decoded_msg = msg.data().decode("utf-8")
            msg_json = json.loads(decoded_msg)
            added_keys, updated_keys, deleted_keys = interpret_message(msg_json, s3_client)
            send_message(producer, msg_json, added_keys, updated_keys, deleted_keys)
            consumer.acknowledge(msg)
        except Exception as err:
            logger.debug(traceback.format_exc())
            logger.error(err)
            consumer.negative_acknowledge(msg)
            if str(err) == "Stop loop":
                break


if __name__ == "__main__":
    pulsar_url = os.environ.get("PULSAR_URL", "")
    consumer_topic = os.environ.get("CONSUMER_TOPIC", "")
    producer_topic = os.environ.get("PRODUCER_TOPIC", "")
    identifier = os.environ.get("IDENTIFIER", "")
    client = pulsar.Client(pulsar_url)
    if identifier:
        identifier = f"-{identifier}"

    consumer = client.subscribe(
        topic=consumer_topic, subscription_name=f"annotations-transformer-consumer{identifier}"
    )

    producer = client.create_producer(
        topic=producer_topic,
        producer_name=f"annotations-transformer-producer{identifier}",
    )

    s3_client = boto3.client("s3")

    main(consumer, producer, s3_client)
