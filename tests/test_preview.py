import unittest
import json
from unittest import mock
from tests.logger import Logger
from tests.prepare_test import SetupTest
from app.commons.service_connection.minio_client import Minio_Client
from app.config import ConfigClass


def get_long_csv():
    data = ""
    for i in range(int(ConfigClass.MAX_PREVIEW_SIZE / 4)):
        data += f"\n{i}, {i+1}, {i+2}, {i+3}, {i+4}, {i+5}"
    return bytes(data, 'utf-8')

class CSVMockClient(object):
    def get_object(self): 
        return MockResponse(type="csv")

class CSV2MockClient(object):
    def get_object(self): 
        return MockResponse(type="csv2")

class CSVLongMockClient(object):
    def get_object(self): 
        return MockResponse(type="csvlong")

class TSVMockClient(object):
    def get_object(self): 
        return MockResponse(type="tsv")

class JSONMockClient(object):
    def get_object(self): 
        return MockResponse(type="json")

class MockResponse(object):
    def __init__(self, type="csv"):
        if type == "csv":
            self.data = b"a,b,c\n1,2,3"
        elif type == "csv2":
            self.data = b"a|b|c\n1|2|3"
        elif type == "csvlong":
            self.data = get_long_csv()
        elif type == "json":
            self.data = b'{"test": "test1"}'
        elif type == "tsv":
            self.data = b"a\tb\tc\n1\t2\t3"


class TestPreview(unittest.TestCase):
    log = Logger(name='test_preview_api.log')
    test = SetupTest(log)

    @classmethod
    def setUpClass(cls):
        cls.log = cls.test.log
        cls.app = cls.test.app
        #cls.dataset = cls.test.create_test_dataset("preview_unit_test")
        file_data1 = {
            "filename": "preview_unittest.csv",
            "dataset_code": "preview_unittest",
            "global_entity_id": "preview_unittest1",
        }
        cls.file1 = cls.test.create_file(file_data1)

        file_data2 = {
            "filename": "preview_unittest.json",
            "dataset_code": "preview_unittest",
            "global_entity_id": "preview_unittest2",
        }
        cls.file2 = cls.test.create_file(file_data2)

        file_data3 = {
            "filename": "preview_unittest2.csv",
            "dataset_code": "preview_unittest",
            "file_size": 4708417,
            "global_entity_id": "preview_unittest3",
        }
        cls.file3 = cls.test.create_file(file_data3)

        file_data4 = {
            "filename": "preview_unittest2.tsv",
            "dataset_code": "preview_unittest",
            "global_entity_id": "preview_unittest4",
        }
        cls.file4 = cls.test.create_file(file_data3)

    @classmethod
    def tearDownClass(cls):
        try:
            cls.log.info("\n")
            cls.log.info("START TEAR DOWN PROCESS")
            #cls.test.delete_test_dataset(cls.dataset["id"])
            cls.test.delete_file_node(cls.file1["id"])
            cls.test.delete_file_node(cls.file2["id"])
            cls.test.delete_file_node(cls.file3["id"])
        except Exception as e:
            cls.log.error("Please manual delete node and entity")
            cls.log.error(e)
            raise e

    @mock.patch('minio.Minio.get_object')
    def test_01_get_csv(self, mock_minio):
        mock_minio.return_value = CSVMockClient().get_object()
        self.log.info("\n")
        self.log.info("01 test get_csv".center(80, '-'))
        file_geid = self.file1["global_entity_id"]
        res = self.app.get(f"/v1/{file_geid}/preview")
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["result"]["content"].replace("\n", "").replace("\r", ""), "a,b,c\n1,2,3".replace("\n", "").replace("\r", ""))


    @mock.patch('minio.Minio.get_object')
    def test_02_get_json(self, mock_minio):
        mock_minio.return_value = JSONMockClient().get_object()
        self.log.info("\n")
        self.log.info("02 test get_json".center(80, '-'))
        file_geid = self.file2["global_entity_id"]
        res = self.app.get(f"/v1/{file_geid}/preview")
        self.assertEqual(res.status_code, 200)
        self.assertEqual(
            json.loads(res.json()["result"]["content"].replace("\n", "").replace("\r", "")), 
            {"test": "test1"}
        )

    @mock.patch('minio.Minio.get_object')
    def test_03_get_csv_pipe(self, mock_minio):
        mock_minio.return_value = CSV2MockClient().get_object()
        self.log.info("\n")
        self.log.info("03 test get_csv_pipe".center(80, '-'))
        file_geid = self.file1["global_entity_id"]
        res = self.app.get(f"/v1/{file_geid}/preview")
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["result"]["content"].replace("\n", "").replace("\r", ""), "a,b,c\r\n1,2,3\r\n".replace("\n", "").replace("\r", ""))

    @mock.patch('minio.Minio.get_object')
    def test_04_get_not_found(self, mock_minio):
        mock_minio.return_value = CSV2MockClient().get_object()
        self.log.info("\n")
        self.log.info("04 test get_not_found".center(80, '-'))
        file_geid = "notfound"
        res = self.app.get(f"/v1/{file_geid}/preview")
        self.assertEqual(res.status_code, 404)
        self.assertEqual(res.json()["error_msg"], "File not found")

    @mock.patch('minio.Minio.get_object')
    def test_05_get_concatinate(self, mock_minio):
        mock_minio.return_value = CSVLongMockClient().get_object()
        self.log.info("\n")
        self.log.info("05 test get_concatinate".center(80, '-'))
        file_geid = self.file3["global_entity_id"]
        res = self.app.get(f"/v1/{file_geid}/preview")
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["result"]["is_concatinated"], True)
        self.assertNotEqual(res.json()["result"]["content"], get_long_csv())

    @mock.patch('minio.Minio.get_object')
    def test_06_get_tsv(self, mock_minio):
        mock_minio.return_value = TSVMockClient().get_object()
        self.log.info("\n")
        self.log.info("06 test get_tsv".center(80, '-'))
        file_geid = self.file4["global_entity_id"]
        res = self.app.get(f"/v1/{file_geid}/preview")
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["result"]["content"], "a,b,c\r\n1,2,3\r\n")
