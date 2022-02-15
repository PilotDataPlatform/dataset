import unittest
import json
from unittest import mock
from tests.logger import Logger
from tests.prepare_test import SetupTest
from app.commons.service_connection.minio_client import Minio_Client
from app.config import ConfigClass
from app.routers.v1.api_version.publish_version import PublishVersion


class TestVersions(unittest.TestCase):
    log = Logger(name='test_versions_api.log')
    test = SetupTest(log)

    @classmethod
    def setUpClass(cls):
        cls.log = cls.test.log
        cls.app = cls.test.app
        cls.dataset = cls.test.create_test_dataset("versionunittest")
        cls.dataset2 = cls.test.create_test_dataset("versionunittest2")

        file_data1 = {
            "filename": "version_unittest.csv",
            "dataset_code": "version_unit_test",
            "global_entity_id": "version_unittest1",
        }
        cls.file1 = cls.test.create_file(file_data1)
        cls.test.add_file_to_dataset(cls.file1["id"], cls.dataset["id"])

    @classmethod
    def tearDownClass(cls):
        try:
            cls.log.info("\n")
            cls.log.info("START TEAR DOWN PROCESS")
            cls.test.delete_test_dataset(cls.dataset["id"])
            cls.test.delete_test_dataset(cls.dataset2["id"])
            cls.test.delete_file_node(cls.file1["id"])
        except Exception as e:
            cls.log.error("Please manual delete node and entity")
            cls.log.error(e)
            raise e

    @mock.patch('app.routers.v1.api_version.publish_version.PublishVersion.download_dataset_files')
    @mock.patch('app.routers.v1.api_version.publish_version.PublishVersion.upload_version')
    def test_01_publish_project(self, mock_upload, mock_download):
        mock_upload.side_effect = "http://minio://fake_version.zip"
        self.log.info("\n")
        self.log.info("01 test publish_project".center(80, '-'))
        dataset_geid = self.dataset["global_entity_id"]
        payload = {
            "operator": "admin",
            "notes": "testing",
            "version": "2.0"
        }
        res = self.app.post(f"/v1/dataset/{dataset_geid}/publish", json=payload)
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["result"]["status_id"], self.dataset["global_entity_id"])

        # Test status
        res = self.app.get(f"/v1/dataset/{dataset_geid}/publish/status?status_id={dataset_geid}")
        self.assertEqual(res.json()["result"]["status"], "success")

    @mock.patch('app.routers.v1.api_version.publish_version.PublishVersion.download_dataset_files')
    @mock.patch('app.routers.v1.api_version.publish_version.PublishVersion.upload_version')
    def test_02_publish_project_large_notes(self, mock_upload, mock_download):
        mock_upload.side_effect = "http://minio://fake_version.zip"
        self.log.info("\n")
        self.log.info("01 test publish_project".center(80, '-'))
        dataset_geid = self.dataset["global_entity_id"]
        payload = {
            "operator": "admin",
            "notes": "".join(["12345" for i in range(60)]),
            "version": "2.0"
        }
        res = self.app.post(f"/v1/dataset/{dataset_geid}/publish", json=payload)
        self.assertEqual(res.status_code, 400)
        self.assertEqual(res.json()["result"], "Notes is to large, limit 250 bytes")

    @mock.patch('app.routers.v1.api_version.publish_version.PublishVersion.download_dataset_files')
    @mock.patch('app.routers.v1.api_version.publish_version.PublishVersion.upload_version')
    def test_03_publish_project_version(self, mock_upload, mock_download):
        mock_upload.side_effect = "http://minio://fake_version.zip"
        self.log.info("\n")
        self.log.info("03 test publish_project".center(80, '-'))
        dataset_geid = self.dataset["global_entity_id"]
        payload = {
            "operator": "admin",
            "notes": "test",
            "version": "incorrect"
        }
        res = self.app.post(f"/v1/dataset/{dataset_geid}/publish", json=payload)
        self.assertEqual(res.status_code, 400)
        self.assertEqual(res.json()["result"], "Incorrect version format")

    @mock.patch('app.routers.v1.api_version.publish_version.PublishVersion.download_dataset_files')
    @mock.patch('app.routers.v1.api_version.publish_version.PublishVersion.upload_version')
    def test_04_publish_project_duplicate(self, mock_upload, mock_download):
        mock_upload.side_effect = "http://minio://fake_version.zip"
        self.log.info("\n")
        self.log.info("03 test publish_project".center(80, '-'))
        dataset_geid = self.dataset["global_entity_id"]
        payload = {
            "operator": "admin",
            "notes": "test",
            "version": "2.0"
        }
        res = self.app.post(f"/v1/dataset/{dataset_geid}/publish", json=payload)
        self.assertEqual(res.status_code, 409)
        self.assertEqual(res.json()["result"], "Duplicate version found for dataset")

    def test_05_version_list(self):
        self.log.info("\n")
        self.log.info("05 test version_list".center(80, '-'))
        dataset_geid = self.dataset["global_entity_id"]
        payload = {
        }
        res = self.app.get(f"/v1/dataset/{dataset_geid}/versions", json=payload)
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["result"][0]["dataset_code"], self.dataset["code"])

    def test_06_version_list(self):
        self.log.info("\n")
        self.log.info("06 test download".center(80, '-'))
        dataset_geid = self.dataset["global_entity_id"]
        payload = {
            "version": "2.0"
        }
        res = self.app.get(f"/v1/dataset/{dataset_geid}/download/pre", params=payload)
        print(res.json())
        self.assertEqual(res.status_code, 200)
        self.assertTrue(isinstance(res.json()["result"]["download_hash"], str))

    @mock.patch('app.routers.v1.api_version.publish_version.PublishVersion.download_dataset_files')
    @mock.patch('app.routers.v1.api_version.publish_version.PublishVersion.upload_version')
    def test_07_publish_project_bad_geid(self, mock_upload, mock_download):
        mock_upload.side_effect = "http://minio://fake_version.zip"
        self.log.info("\n")
        self.log.info("07 test publish_project".center(80, '-'))
        dataset_geid = "badgeid"
        payload = {
            "operator": "admin",
            "notes": "test",
            "version": "2.0"
        }
        res = self.app.post(f"/v1/dataset/{dataset_geid}/publish", json=payload)
        self.assertEqual(res.status_code, 404)
        self.assertEqual(res.json()["error_msg"], "Dataset not found")

    def test_08_status_not_found(self):
        dataset_geid = self.dataset2["global_entity_id"]
        res = self.app.get(f"/v1/dataset/{dataset_geid}/publish/status?status_id={dataset_geid}")
        self.assertEqual(res.status_code, 404)
        self.assertEqual(res.json()["error_msg"], "Status not found")
