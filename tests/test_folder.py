import unittest
from tests.logger import Logger
from tests.prepare_test import SetupTest
from app.config import ConfigClass
from app.resources.neo4j_helper import query_relation


class TestDatasetFolder(unittest.TestCase):
    log = Logger(name='test_dataset_folder_api.log')
    test = SetupTest(log)

    @classmethod
    def setUpClass(cls):
        cls.log = cls.test.log
        cls.app = cls.test.app
        cls.dataset = cls.test.create_test_dataset("datasetfolderunittest")
        cls.files = []

    @classmethod
    def tearDownClass(cls):
        try:
            cls.log.info("\n")
            cls.log.info("START TEAR DOWN PROCESS")
            cls.test.delete_test_dataset(cls.dataset["id"])
            for file in cls.files:
                cls.test.delete_file_node(file["id"])
        except Exception as e:
            cls.log.error("Please manual delete node and entity")
            cls.log.error(e)
            raise e

    def test_01_create_folder_root(self):
        self.log.info("\n")
        self.log.info("01 test create_folder_root".center(80, '-'))
        dataset_geid = self.dataset["global_entity_id"]
        payload = {
            "folder_name": "unitest_folder",
            "username": "admin",
        }
        res = self.app.post(f"/v1/dataset/{dataset_geid}/folder", json=payload)
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["result"]["name"], "unitest_folder")
        self.assertEqual(res.json()["result"]["folder_level"], 0)
        self.assertEqual(res.json()["result"]["dataset_code"], self.dataset["code"])
        relation = query_relation(
            "own", 
            "Dataset", 
            "Folder", 
            start_params={"global_entity_id": self.dataset["global_entity_id"]},
            end_params={"global_entity_id": res.json()["result"]["global_entity_id"]}
        )[0]
        self.assertEqual(relation["r"]["type"], "own")
        self.files.append(res.json()["result"])


    def test_02_duplicate_create_folder_root(self):
        self.log.info("\n")
        self.log.info("02 test duplicate_create_folder_root".center(80, '-'))
        dataset_geid = self.dataset["global_entity_id"]
        payload = {
            "folder_name": "unitest_folder",
            "username": "admin",
        }
        res = self.app.post(f"/v1/dataset/{dataset_geid}/folder", json=payload)
        self.assertEqual(res.status_code, 409)
        self.assertEqual(res.json()["error_msg"], "folder with that name already exists")

    def test_03_create_sub_folder(self):
        self.log.info("\n")
        self.log.info("03 test create_sub_folder".center(80, '-'))
        dataset_geid = self.dataset["global_entity_id"]
        payload = {
            "folder_name": "unitest_folder2",
            "username": "admin",
            "parent_folder_geid": self.files[0]["global_entity_id"], 
        }
        res = self.app.post(f"/v1/dataset/{dataset_geid}/folder", json=payload)
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["result"]["name"], "unitest_folder2")
        self.assertEqual(res.json()["result"]["folder_level"], 1)
        self.assertEqual(res.json()["result"]["dataset_code"], self.dataset["code"])
        self.files.append(res.json()["result"])

    def test_04_create_folder_invalid_name(self):
        self.log.info("\n")
        self.log.info("04 test create_folder_invalid_name".center(80, '-'))
        dataset_geid = self.dataset["global_entity_id"]
        payload = {
            "folder_name": " unittest_/dataset_folder",
            "username": "admin",
        }
        res = self.app.post(f"/v1/dataset/{dataset_geid}/folder", json=payload)
        self.assertEqual(res.status_code, 400)
        self.assertEqual(res.json()["error_msg"], "Invalid folder name")

    def test_05_create_folder_dataset_404(self):
        self.log.info("\n")
        self.log.info("05 test create_folder_dataset_404".center(80, '-'))
        dataset_geid = "invalid"
        payload = {
            "folder_name": "unitest_folder",
            "username": "admin",
        }
        res = self.app.post(f"/v1/dataset/{dataset_geid}/folder", json=payload)
        self.assertEqual(res.status_code, 404)
        self.assertEqual(res.json()["error_msg"], "Dataset not found")

    def test_06_create_folder_404(self):
        self.log.info("\n")
        self.log.info("06 test create_folder_404".center(80, '-'))
        dataset_geid = self.dataset["global_entity_id"]
        payload = {
            "folder_name": "unitest_folder",
            "username": "admin",
            "parent_folder_geid": "invalid", 
        }
        res = self.app.post(f"/v1/dataset/{dataset_geid}/folder", json=payload)
        self.assertEqual(res.status_code, 404)
        self.assertEqual(res.json()["error_msg"], "Folder not found")
