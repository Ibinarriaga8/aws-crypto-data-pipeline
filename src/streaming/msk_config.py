from typing import List

import boto3


def get_msk_bootstrap_servers(
    cluster_arn: str,
    region_name: str = "eu-south-2",
) -> List[str]:
    """
    Retrieve the bootstrap broker endpoints for an AWS MSK cluster.

    Args:
        cluster_arn (str): ARN of the MSK cluster.
        region_name (str): AWS region where the cluster is deployed.

    Returns:
        List[str]: List of Kafka bootstrap servers.

    Raises:
        KeyError: If the response does not contain expected broker information.
        Exception: If AWS request fails.
    """
    kafka_client = boto3.client("kafka", region_name=region_name)

    response = kafka_client.get_bootstrap_brokers(ClusterArn=cluster_arn)

    brokers_string = response.get("BootstrapBrokerString")

    if not brokers_string:
        raise KeyError("BootstrapBrokerString not found in MSK response.")

    return brokers_string.split(",")


def get_kafka_producer(
    bootstrap_servers: List[str],
):
    """
    Create a Kafka producer using the kafka-python library.

    Args:
        bootstrap_servers (List[str]): List of Kafka broker endpoints.

    Returns:
        KafkaProducer: Configured Kafka producer instance.
    """
    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: v.encode("utf-8"),
    )