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
                    return_value=b'{"result": {"product_information": {"product_details": {"grade": "good", "ref": "https://test"}, "availability_and_accessability": {"grade": "basic", "ref": "https://test"}}}}'
                )
            )
        }
        result = get_file_from_s3(mock_s3_client, "bucket", "key")
        self.assertEqual(
            result,
            '{"result": {"product_information": {"product_details": {"grade": "good", "ref": "https://test"}, "availability_and_accessability": {"grade": "basic", "ref": "https://test"}}}}',
        )

    @patch("annotations_transformer.__main__.boto3.client")
    def test_upload_file_to_s3(self, mock_boto_client):
        mock_s3_client = mock_boto_client.return_value
        upload_file_to_s3(mock_s3_client, "bucket", "key", "data")
        mock_s3_client.put_object.assert_called_once_with(Bucket="bucket", Key="key", Body="data")

    def test_json_to_rdf(self):
        json_data = {
            "result": {
                "product_information": {
                    "product_details": {"grade": "good", "ref": "https://test"},
                    "availability_and_accessability": {"grade": "basic", "ref": "https://test"},
                }
            }
        }
        rdf_data = json_to_rdf(json_data)
        expected_rdf = (
            "@prefix ns1: <http://namespace-url.org/> .\n\n"
            "ns1:result ns1:product_information ns1:product_information .\n\n"
            'ns1:availability_and_accessability ns1:grade "basic" ;\n'
            "    ns1:ref <https://test> .\n\n"
            'ns1:product_details ns1:grade "good" ;\n'
            "    ns1:ref <https://test> .\n\n"
            "ns1:product_information ns1:availability_and_accessability ns1:availability_and_accessability ;\n"
            "    ns1:product_details ns1:product_details ."
        )
        self.assertIn(expected_rdf, rdf_data)

    def test_get_transformed_key(self):
        result = get_transformed_key("source/key.json", "source", "target")
        self.assertEqual(result, "transformed/target/key.ttl")

    @patch("annotations_transformer.__main__.boto3.client")
    def test_delete_key(self, mock_boto_client):
        mock_s3_client = mock_boto_client.return_value
        result = delete_key(mock_s3_client, "bucket", "key/file.json", "key", "datasets/key")
        self.assertEqual(result, ["transformed/datasets/key/file.ttl"])
        mock_s3_client.delete_object.assert_called_once_with(
            Bucket="bucket", Key="transformed/datasets/key/file.ttl"
        )

    @patch("annotations_transformer.__main__.boto3.client")
    def test_reformat_annotations(self, mock_boto_client):
        mock_s3_client = mock_boto_client.return_value
        mock_s3_client.get_object.return_value = {
            "Body": MagicMock(
                read=MagicMock(
                    return_value=b'{"result": {"product_information": {"product_details": {"grade": "good", "ref": "https://test"}, "availability_and_accessability": {"grade": "basic", "ref": "https://test"}}}}'
                )
            )
        }
        result = reformat_annotations(
            mock_s3_client, "bucket", "source/key.json", "source", "target"
        )
        self.assertEqual(result, ["transformed/target/key.ttl"])

    @patch("annotations_transformer.__main__.boto3.client")
    def test_interpret_message(self, mock_boto_client):
        mock_s3_client = mock_boto_client.return_value
        mock_s3_client.put_object.return_value = None
        mock_s3_client.delete_object.return_value = None
        mock_s3_client.get_object.return_value = {
            "Body": MagicMock(
                read=MagicMock(
                    return_value=b'{"result": {"product_information": {"product_details": {"grade": "good", "ref": "https://test"}, "availability_and_accessability": {"grade": "basic", "ref": "https://test"}}}}'
                )
            )
        }
        msg_json = {
            "added_keys": ["source/key_1.json"],
            "updated_keys": ["source/key_2.json"],
            "deleted_keys": ["source/key_3.json"],
            "bucket_name": "bucket",
            "source": "source",
            "target": "target",
        }
        added_keys, updated_keys, deleted_keys = interpret_message(msg_json, mock_s3_client)
        self.assertEqual(added_keys, ["transformed/target/key_1.ttl"])
        self.assertEqual(updated_keys, ["transformed/target/key_2.ttl"])
        self.assertEqual(deleted_keys, ["transformed/target/key_3.ttl"])

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
            ["transformed/target/key_1.ttl"],
            ["transformed/target/key_2.ttl"],
            ["transformed/target/key_3.ttl"],
        )

        output_message = {
            "added_keys": ["transformed/target/key_1.ttl"],
            "updated_keys": ["transformed/target/key_2.ttl"],
            "deleted_keys": ["transformed/target/key_3.ttl"],
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
                    return_value=b'{"result": {"product_information": {"product_details": {"grade": "good", "ref": "https://test"}, "availability_and_accessability": {"grade": "basic", "ref": "https://test"}}}}'
                )
            )
        }
        message = {
            "added_keys": ["source/key_1.json"],
            "updated_keys": ["source/key_2.json"],
            "deleted_keys": ["source/key_3.json"],
            "bucket_name": "bucket",
            "source": "source",
            "target": "target",
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
            "added_keys": ["transformed/target/key_1.ttl"],
            "updated_keys": ["transformed/target/key_2.ttl"],
            "deleted_keys": ["transformed/target/key_3.ttl"],
            "bucket_name": "bucket",
            "source": "source",
            "target": "target",
        }

        mock_producer.send.assert_called()
        mock_producer.send.assert_called_with(json.dumps(output_message).encode("utf-8"))


if __name__ == "__main__":
    unittest.main()
