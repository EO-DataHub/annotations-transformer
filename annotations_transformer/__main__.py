import json
import logging
import os
import time
import traceback
import urllib.parse
from urllib.parse import urlparse

import boto3
import botocore.client
import botocore.exceptions
import pulsar
from rdflib import Dataset, Literal, Namespace, URIRef

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

ADES_URL = os.getenv("ADES_URL", "https://test.eodatahub.org.uk/ades")

RC_URL = os.getenv("RC_URL", "https://dev.eodatahub.org.uk/api/catalogue/stac/catalogs")


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


def is_url(value: str) -> bool:
    try:
        result = urlparse(value)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False


def json_to_rdf(json_data: dict, workspace: str, workflow_id: str, job_id: str) -> bytes:
    g = Dataset()
    dcat = Namespace("http://www.w3.org/ns/dcat#")
    dct = Namespace("http://purl.org/dc/terms/")
    dctype = Namespace("http://purl.org/dc/dcmitype/")
    foaf = Namespace("http://xmlns.com/foaf/0.1/")
    owl = Namespace("http://www.w3.org/2002/07/owl#")
    prov = Namespace("http://www.w3.org/ns/prov#")
    rdf = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
    rdfs = Namespace("http://www.w3.org/2000/01/rdf-schema#")
    sdo = Namespace("http://schema.org/")
    sdmx_attribute = Namespace("http://purl.org/linked-data/sdmx/2009/attribute#")
    skos = Namespace("http://www.w3.org/2004/02/skos/core#")
    vcard = Namespace("http://www.w3.org/2006/vcard/ns#")
    xsd = Namespace("http://www.w3.org/2001/XMLSchema#")
    dcterms = Namespace("http://purl.org/dc/terms/")
    vann = Namespace("http://purl.org/vocab/vann/")
    qb = Namespace("http://purl.org/linked-data/cube#")
    daq = Namespace("http://purl.org/eis/vocab/daq#")
    voaf = Namespace("http://purl.org/vocommons/voaf#")
    oa = Namespace("http://www.w3.org/ns/oa#")
    duv = Namespace("http://www.w3.org/ns/duv#")
    dqv = Namespace("http://www.w3.org/ns/dqv#")
    eodhqa = Namespace("https://eodatahub.org.uk/api/ontologies/qa/")
    eodhweblinks = Namespace("https://eodatahub.org.uk/api/ontologies/weblinks/")

    g.bind("dcat", dcat)
    g.bind("dct", dct)
    g.bind("dctype", dctype)
    g.bind("foaf", foaf)
    g.bind("owl", owl)
    g.bind("prov", prov)
    g.bind("rdf", rdf)
    g.bind("rdfs", rdfs)
    g.bind("sdo", sdo)
    g.bind("sdmx-attribute", sdmx_attribute)
    g.bind("skos", skos)
    g.bind("vcard", vcard)
    g.bind("xsd", xsd)
    g.bind("dcterms", dcterms)
    g.bind("vann", vann)
    g.bind("qb", qb)
    g.bind("daq", daq)
    g.bind("voaf", voaf)
    g.bind("oa", oa)
    g.bind("duv", duv)
    g.bind("dqv", dqv)
    g.bind("eodhqa", eodhqa)
    g.bind("eodhweblinks", eodhweblinks)

    try:
        results = json_data["result"]
    except KeyError:
        logger.error("No result key in JSON annotations data")
        return None

    # Define URIs
    check_run = Literal("checkRun")
    quality_check_results = Literal(":qualityCheckResults")
    org = Literal("_:org")
    author = Literal("_:author")

    givenName = (
        Literal(json_data["check_author"], datatype=xsd.string)
        if "check_author" in json_data
        else Literal("Unknown", datatype=xsd.string)
    )
    orgName = (
        Literal(json_data["org_name"], datatype=xsd.string)
        if "org_name" in json_data
        else Literal("Unknown", datatype=xsd.string)
    )

    # Add org details
    g.add((org, rdf.type, prov.Agent))
    g.add((org, rdf.type, prov.Organization))
    g.add((org, foaf.name, orgName))

    # Add author details
    g.add((author, rdf.type, prov.Agent))
    g.add((author, rdf.type, prov.Person))
    g.add((author, foaf.givenName, givenName))
    g.add((author, prov.actedOnBehalfOf, org))

    # Add process details
    execution_url = f"{ADES_URL}/{workspace}/ogc-api/processes/{workflow_id}"
    exec_url = URIRef(execution_url)
    g.add((exec_url, rdf.type, prov.Entity))

    # Add job details
    # Add process details
    job_url = f"{ADES_URL}/{workspace}/ogc-api/jobs/{job_id}"
    j_url = URIRef(job_url)
    g.add((j_url, rdf.type, prov.Activity))
    g.add((j_url, prov.used, exec_url))
    g.add((j_url, prov.wasAssociatedWith, author))

    # Add QA details

    # Handle Result

    # Create the named graph
    quality_check_results = Literal(":qualityCheckResults")
    quality_check_results_graph = g.get_context(quality_check_results)

    # Add the check run details
    quality_check_results_graph.add((check_run, rdf.type, eodhqa.EODHQualityMeasurementDataset))
    check_name = json_data["check_name"] if "check_name" in json_data else "Unknown"
    uuid = json_data["UUID"] if "UUID" in json_data else "Unknown"
    label = f"{check_name} {uuid}"

    quality_check_results_graph.add((check_run, rdfs.label, Literal(label)))
    quality_check_results_graph.add((check_run, owl.sameAs, URIRef(f"urn:uuid:{uuid}")))

    dataset = json_data["data_collection"] if "data_collection" in json_data else "Unknown"

    if is_url(dataset):
        datasetURI = URIRef(dataset)
    else:
        datasetURI = URIRef(urllib.parse.quote(f"{RC_URL}/supported-datasets/{dataset}", safe=":/"))

    quality_check_results_graph.add((check_run, eodhqa.datasetComputedOn, datasetURI))

    check_datetime_validity_end = "2024-10-11T09:15:14.056435Z"

    quality_check_results_graph.add(
        (check_run, eodhqa.validityEnd, Literal(check_datetime_validity_end, datatype=xsd.dateTime))
    )

    if "ref_url" in json_data:
        for key, value in json_data["ref_url"].items():
            doc = Literal(f":{key}")
            doc_type = "https://www.iana.org/assignments/media-types/application/pdf"
            quality_check_results_graph.add((doc, rdf.type, eodhweblinks.WebLink))
            quality_check_results_graph.add((doc, eodhweblinks.type, URIRef(doc_type)))
            doc_title = "Placeholder Title"
            doc_lang = "en"
            quality_check_results_graph.add(
                (doc, eodhweblinks.title, Literal(doc_title, lang=doc_lang))
            )
            # file_path = f"processing-results/{job_id}/{key}.pdf"
            # qa_link = f"https://{workspace}.eodatahub-workspaces.org.uk/files/{file_path}"
            qa_link = value
            quality_check_results_graph.add((doc, eodhweblinks.href, Literal(qa_link)))
            quality_check_results_graph.add((check_run, eodhqa.weblink, doc))

    results = json_data["result"]

    for key, value in results.items():
        # Construct subgraph
        # graph_title = Literal(f":{key}")
        # subgraph = g.get_context(graph_title)
        measures = Literal(key)
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                sub_subject = Literal(f":{sub_key}")
                quality_check_results_graph.add((sub_subject, rdf.type, dqv.QualityMeasurement))
                quality_check_results_graph.add(
                    (sub_subject, sdmx_attribute.unitMeasure, eodhqa.gradeScale)
                )
                quality_check_results_graph.add(
                    (sub_subject, dqv.isMeasurementOf, eodhqa[measures])
                )
                quality_check_results_graph.add((sub_subject, dqv.computedOn, datasetURI))
                quality_check_results_graph.add((sub_subject, qb.dataSet, check_run))
                grade = sub_value.get("grade")
                if grade == "good":
                    grade_value = eodhqa.gradeGood
                elif grade == "basic":
                    grade_value = eodhqa.gradeBasic
                elif grade == "excellent":
                    grade_value = eodhqa.gradeExcellent
                elif grade == "ideal":
                    grade_value = eodhqa.gradeIdeal
                else:
                    grade_value = eodhqa.gradeUnknown
                quality_check_results_graph.add((sub_subject, dqv.value, grade_value))

    # Add the quality check results
    g.add((quality_check_results, rdf.type, eodhqa.QualityMetadata))
    g.add((quality_check_results, rdf.type, prov.Entity))
    generated_time = (
        json_data["check_datetime"]
        if "check_datetime" in json_data
        else time.strftime("%Y-%m-%dT%H:%M:%S")
    )
    g.add(
        (
            quality_check_results,
            prov.generatedAtTime,
            Literal(generated_time, datatype=xsd.dateTime),
        )
    )
    g.add((quality_check_results, prov.wasGeneratedBy, j_url))
    g.add((quality_check_results, prov.wasAttributedTo, author))

    return g.serialize(format="trig")


def get_transformed_key(key: str, source: str, target: str) -> str:
    new_key = key.replace(source, target)
    new_key = new_key.replace(".json", ".trig")
    dest_key = f"transformed/{new_key}"
    return dest_key


def delete_key(
    s3_client: botocore.client.BaseClient, bucket_name: str, key: str, source: str, target: str
) -> list:
    dest_key = get_transformed_key(key, source, target)
    try:
        s3_client.delete_object(Bucket=bucket_name, Key=dest_key)
        return dest_key
    except botocore.exceptions.ClientError as e:
        logger.error(f"Error deleting object {dest_key} from bucket {bucket_name}")
        logger.error(e)
        return


def reformat_annotations(
    s3_client: botocore.client.BaseClient,
    bucket_name: str,
    workspace: str,
    key: str,
    source: str,
    target: str,
    workflow_id: str,
    job_id: str,
) -> list:
    logger.info(f"Reformatting annotations for key {key}")
    file = get_file_from_s3(s3_client, bucket_name, key)
    if file is None:
        return
    # Load the file as a dictionary
    annotations = json.loads(file)

    # Reformat annotations into RDF
    rdf_data = json_to_rdf(annotations, workspace, workflow_id, job_id)
    if not rdf_data:
        return

    # Generate transformed key
    dest_key = get_transformed_key(key, source, target)

    # Upload the transformed data to S3
    upload_file_to_s3(s3_client, bucket_name, dest_key, rdf_data)

    return dest_key


def interpret_message(msg_json: dict, s3_client) -> tuple:

    # Extract keys
    added_keys = msg_json["added_keys"]
    updated_keys = msg_json["updated_keys"]
    deleted_keys = msg_json["deleted_keys"]

    # Extract bucket
    bucket_name = msg_json["bucket_name"]

    # Extract workspace
    workspace = msg_json["workspace"]

    # Extract source and target
    source = msg_json["source"]
    target = msg_json["target"]

    # Extract data from workflow outputs, if available
    workflow_id = msg_json["workflow_id"] if "workflow_id" in msg_json else "Unknown"
    job_id = msg_json["job_id"] if "job_id" in msg_json else "Unknown"

    output_added_keys = []
    output_updated_keys = []
    output_deleted_keys = []

    for key in added_keys:
        output_added_keys.append(
            reformat_annotations(
                s3_client, bucket_name, workspace, key, source, target, workflow_id, job_id
            )
        )
    for key in updated_keys:
        output_updated_keys.append(
            reformat_annotations(
                s3_client, bucket_name, workspace, key, source, target, workflow_id, job_id
            )
        )
    for key in deleted_keys:
        output_deleted_keys.append(delete_key(s3_client, bucket_name, key, source, target))

    return output_added_keys, output_updated_keys, output_deleted_keys


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
