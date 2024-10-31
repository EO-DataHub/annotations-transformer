import json
import unittest
from unittest.mock import MagicMock, patch

from annotations_transformer.__main__ import (
    delete_key,
    get_file_from_s3,
    get_transformed_key,
    interpret_message,
    json_to_rdf,
    main,
    reformat_annotations,
    send_message,
    upload_file_to_s3,
)


class TestAnnotationsTransformer(unittest.TestCase):

    @patch("annotations_transformer.__main__.boto3.client")
    def test_get_file_from_s3(self, mock_boto_client):
        mock_s3_client = mock_boto_client.return_value
        mock_s3_client.get_object.return_value = {
            "Body": MagicMock(
                read=MagicMock(
                    return_value=b"""{"result": {"product_information": {"product_details": {"grade": "good", "ref": "https://test"}, "availability_and_accessability": {"grade": "basic", "ref": "https://test"}}}}"""  # noqa: E501
                )
            )
        }
        result = get_file_from_s3(mock_s3_client, "bucket", "key")
        self.assertEqual(
            result,
            """{"result": {"product_information": {"product_details": {"grade": "good", "ref": "https://test"}, "availability_and_accessability": {"grade": "basic", "ref": "https://test"}}}}""",  # noqa: E501
        )

    @patch("annotations_transformer.__main__.boto3.client")
    def test_upload_file_to_s3(self, mock_boto_client):
        mock_s3_client = mock_boto_client.return_value
        upload_file_to_s3(mock_s3_client, "bucket", "key", "data")
        mock_s3_client.put_object.assert_called_once_with(Bucket="bucket", Key="key", Body="data")

    def test_json_to_rdf(self):
        json_data = {
            "data_collection": "Sentinel-2 L1C",
            "check_name": "documentation review",
            "UUID": "uuid",
            "result": {
                "product_information": {
                    "product_details": {"grade": "good", "ref": "https://test"},
                },
                "metrology": {
                    "radiometric_calibration_and_characterisation)": {
                        "grade": "excellent",
                        "ref": "https://asdf",
                    },
                },
            },
            "result_vocab": "vocab/url",
            "check_datetime": "2024-10-11T09:15:14.056435Z",
            "check_datetime_validity_end": "2025-10-11T09:15:14.057873Z",
            "check_author": "S Malone",
            "check_version": 1.0,
            "ref_url": {"documentation_1": "url", "documentation_2": "url"},
        }
        rdf_data = json_to_rdf(json_data, "test-workspace", "workflowID", "jobID")

        # Normalize whitespace and compare
        def normalize_whitespace(s):
            return " ".join(s.split())

        self.assertIsNotNone(rdf_data)

    def test_get_transformed_key(self):
        result = get_transformed_key("source/key.json", "source", "target")
        self.assertEqual(result, "transformed/target/key.trig")

    @patch("annotations_transformer.__main__.boto3.client")
    def test_delete_key(self, mock_boto_client):
        mock_s3_client = mock_boto_client.return_value
        result = delete_key(mock_s3_client, "bucket", "key/file.json", "key", "datasets/key")
        self.assertEqual(result, "transformed/datasets/key/file.trig")
        mock_s3_client.delete_object.assert_called_once_with(
            Bucket="bucket", Key="transformed/datasets/key/file.trig"
        )

    @patch("annotations_transformer.__main__.boto3.client")
    def test_reformat_annotations(self, mock_boto_client):
        mock_s3_client = mock_boto_client.return_value
        mock_s3_client.get_object.return_value = {
            "Body": MagicMock(
                read=MagicMock(
                    return_value=b"""{"result": {"product_information": {"product_details":
                    {"grade": "good", "ref": "https://test"}, "availability_and_accessability":
                    {"grade": "basic", "ref": "https://test"}}}}"""
                )
            )
        }
        result = reformat_annotations(
            mock_s3_client,
            "bucket",
            "test-workspace",
            "source/key.json",
            "source",
            "target",
            "example-workflow",
            "1234-5678",
        )
        self.assertEqual(result, "transformed/target/key.trig")

    @patch("annotations_transformer.__main__.boto3.client")
    def test_interpret_message(self, mock_boto_client):
        mock_s3_client = mock_boto_client.return_value
        mock_s3_client.put_object.return_value = None
        mock_s3_client.delete_object.return_value = None
        mock_s3_client.get_object.return_value = {
            "Body": MagicMock(
                read=MagicMock(
                    return_value=b"""{"result": {"product_information": {"product_details":
                    {"grade": "good", "ref": "https://test"},"availability_and_accessability":
                    {"grade": "basic", "ref": "https://test"}}}}"""
                )
            )
        }
        msg_json = {
            "workspace": "test-workspace",
            "added_keys": ["source/key_1.json"],
            "updated_keys": ["source/key_2.json"],
            "deleted_keys": ["source/key_3.json"],
            "bucket_name": "bucket",
            "source": "source",
            "target": "target",
            "workflow_id": "example-workflow",
            "job_id": "1234-5678",
        }
        added_keys, updated_keys, deleted_keys = interpret_message(msg_json, mock_s3_client)
        self.assertEqual(added_keys, ["transformed/target/key_1.trig"])
        self.assertEqual(updated_keys, ["transformed/target/key_2.trig"])
        self.assertEqual(deleted_keys, ["transformed/target/key_3.trig"])

    @patch("annotations_transformer.__main__.pulsar.Producer")
    def test_send_message(self, mock_producer):
        producer = mock_producer.return_value
        input_message = {
            "added_keys": ["source/key_1.json"],
            "updated_keys": ["source/key_2.json"],
            "deleted_keys": ["source/key_3.json"],
            "bucket_name": "bucket",
            "source": "source",
            "target": "target",
        }

        send_message(
            producer,
            input_message,
            ["transformed/target/key_1.trig"],
            ["transformed/target/key_2.trig"],
            ["transformed/target/key_3.trig"],
        )

        output_message = {
            "added_keys": ["transformed/target/key_1.trig"],
            "updated_keys": ["transformed/target/key_2.trig"],
            "deleted_keys": ["transformed/target/key_3.trig"],
            "bucket_name": "bucket",
            "source": "source",
            "target": "target",
        }

        producer.send.assert_called_once()
        producer.send.assert_called_with(json.dumps(output_message).encode("utf-8"))

    @patch("annotations_transformer.__main__.pulsar.Client")
    @patch("annotations_transformer.__main__.boto3.client")
    def test_main(self, mock_boto_client, mock_pulsar_client):
        mock_consumer = MagicMock()
        mock_producer = MagicMock()
        mock_s3_client = mock_boto_client.return_value
        mock_s3_client.put_object.return_value = None
        mock_s3_client.delete_object.return_value = None
        mock_s3_client.get_object.return_value = {
            "Body": MagicMock(
                read=MagicMock(
                    return_value=b"""{"result": {"product_information": {"product_details": {"grade": "good",
                    "ref": "https://test"}, "availability_and_accessability":
                    {"grade": "basic", "ref": "https://test"}}}}"""
                )
            )
        }
        message = {
            "workspace": "test-workspace",
            "added_keys": ["source/key_1.json"],
            "updated_keys": ["source/key_2.json"],
            "deleted_keys": ["source/key_3.json"],
            "bucket_name": "bucket",
            "source": "source",
            "target": "target",
            "workflow_id": "example-workflow",
            "job_id": "1234-5678",
        }
        message_str = json.dumps(message).encode("UTF-8")
        pulsar_input = MagicMock(data=lambda: message_str)
        mock_consumer.receive.side_effect = [pulsar_input, Exception("Stop loop")]
        mock_pulsar_client.return_value.subscribe.return_value = mock_consumer
        mock_pulsar_client.return_value.create_producer.return_value = mock_producer

        main(mock_consumer, mock_producer, mock_s3_client)

        mock_consumer.receive.assert_called()
        mock_consumer.acknowledge.assert_called_with(pulsar_input)

        mock_consumer.negative_acknowledge.assert_called_once()

        output_message = {
            "workspace": "test-workspace",
            "added_keys": ["transformed/target/key_1.trig"],
            "updated_keys": ["transformed/target/key_2.trig"],
            "deleted_keys": ["transformed/target/key_3.trig"],
            "bucket_name": "bucket",
            "source": "source",
            "target": "target",
            "workflow_id": "example-workflow",
            "job_id": "1234-5678",
        }

        mock_producer.send.assert_called()
        mock_producer.send.assert_called_with(json.dumps(output_message).encode("utf-8"))


if __name__ == "__main__":
    unittest.main()
