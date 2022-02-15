import unittest
import json
from unittest import mock
from tests.logger import Logger
from tests.prepare_test import SetupTest
import requests
from app.config import ConfigClass

class TestSchema(unittest.TestCase):
    log = Logger(name='test_schema.log')
    test = SetupTest(log)

    @classmethod
    def setUpClass(cls):
        cls.log = cls.test.log
        cls.app = cls.test.app
        cls.dataset = cls.test.create_test_dataset("schemaunittest")
        cls.schema_template = cls.test.create_schema_template(cls.dataset["global_entity_id"])
        cls.schemas = []

    @classmethod
    def tearDownClass(cls):
        try:
            cls.log.info("\n")
            cls.log.info("START TEAR DOWN PROCESS")
            cls.test.delete_test_dataset(cls.dataset["id"])
            cls.test.delete_schema_template(cls.dataset["global_entity_id"], cls.schema_template["geid"])
            for schema in cls.schemas:
                cls.test.delete_schema(cls.dataset["global_entity_id"], schema["geid"])
        except Exception as e:
            cls.log.error("Please manual delete node and entity")
            cls.log.error(e)
            raise e

    def test_01_create_schema(self):
        self.log.info("\n")
        self.log.info("01 test create_schema".center(80, '-'))
        dataset_geid = self.dataset["global_entity_id"]
        payload = {
            "name": "unittestdataset",
            "dataset_geid": dataset_geid,
            "tpl_geid": self.schema_template["geid"],
            "standard": "default",
            "system_defined": True,
            "is_draft": True,
            "content": {},
            "creator": "admin",
            "activity": [
                {
                  "action": "CREATE",
                  "resource": "Schema",
                  "detail": {
                      "name": "essential.schema.json"
                  }
                }
            ],
        }
        res = self.app.post(f"/v1/schema", json=payload)
        self.schemas.append(res.json()["result"])
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["result"]["name"], "unittestdataset")

    def test_02_get_schema(self):
        self.log.info("\n")
        self.log.info("02 test get_schema".center(80, '-'))
        dataset_geid = self.dataset["global_entity_id"]
        schema_geid = self.schemas[0]["geid"]
        res = self.app.get(f"/v1/schema/{schema_geid}")
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["result"], self.schemas[0])

    def test_03_update_schema(self):
        self.log.info("\n")
        self.log.info("03 test update_schema".center(80, '-'))
        dataset_geid = self.dataset["global_entity_id"]
        schema_geid = self.schemas[0]["geid"]
        payload = {
            "username": "admin",
            "content": {"test": "testing"},
            "activity": [],
        }
        res = self.app.put(f"/v1/schema/{schema_geid}", json=payload)
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["result"]["content"], {"test": "testing"})

    def test_04_create_essential_schema(self):
        self.log.info("\n")
        self.log.info("01 test create_essential_schema".center(80, '-'))

        # Get created essential schema
        payload = {
            "dataset_geid": self.dataset["global_entity_id"],
            "name": "essential.schema.json",
        }
        response = self.app.post("/v1/schema/list", json=payload)
        schema_geid = response.json()["result"][0]["geid"]
        self.test.delete_schema(self.dataset["global_entity_id"], schema_geid)

        dataset_geid = self.dataset["global_entity_id"]
        payload = {
            "name": "essential.schema.json",
            "dataset_geid": dataset_geid,
            "tpl_geid": self.schema_template["geid"],
            "standard": "default",
            "system_defined": True,
            "is_draft": True,
            "content": {},
            "creator": "admin",
            "activity": [],
        }
        res = self.app.post(f"/v1/schema", json=payload)
        self.schemas.append(res.json()["result"])
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["result"]["name"], "essential.schema.json")

    def test_05_update_essential_schema(self):
        self.log.info("\n")
        self.log.info("01 test create_essential_schema".center(80, '-'))
        dataset_geid = self.dataset["global_entity_id"]
        schema_geid = self.schemas[1]["geid"]
        payload = {
            "username": "admin",
            "content": {
                "dataset_title": "testing",
                "dataset_authors": ["testing"],
                "dataset_description": "testing",
                "dataset_type": "testing",
                "dataset_modality": "testing",
            },
            "activity": [],
        }
        res = self.app.put(f"/v1/schema/{schema_geid}", json=payload)
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["result"]["content"]["dataset_title"], "testing")

        id = self.dataset["id"]
        response = requests.get(ConfigClass.NEO4J_SERVICE + f"nodes/Dataset/node/{id}")
        self.assertEqual(response.json()[0]["title"], "testing")
        self.assertEqual(response.json()[0]["type"], "testing")

    def test_06_delete_schema(self):
        self.log.info("\n")
        self.log.info("06 test delete_schema".center(80, '-'))
        dataset_geid = self.dataset["global_entity_id"]
        schema_geid = self.schemas.pop()["geid"]
        payload = {
            "username": "admin",
            "dataset_geid": dataset_geid,
            "activity": [],
        }
        res = self.app.delete(f"/v1/schema/{schema_geid}", json=payload)
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["result"], "success")

    def test_07_get_schema_404(self):
        self.log.info("\n")
        self.log.info("07 test get_schema_404".center(80, '-'))
        dataset_geid = self.dataset["global_entity_id"]
        schema_geid = "notfound"
        res = self.app.get(f"/v1/schema/{schema_geid}")
        self.assertEqual(res.status_code, 404)
        self.assertEqual(res.json()["error_msg"], "Schema not found")

    def test_08_create_duplicate(self):
        self.log.info("\n")
        self.log.info("08 test create_duplicate".center(80, '-'))
        dataset_geid = self.dataset["global_entity_id"]
        payload = {
            "name": "unittestdataset",
            "dataset_geid": dataset_geid,
            "tpl_geid": self.schema_template["geid"],
            "standard": "default",
            "system_defined": True,
            "is_draft": True,
            "content": {},
            "creator": "admin",
            "activity": [],
        }
        res = self.app.post(f"/v1/schema", json=payload)
        self.assertEqual(res.status_code, 409)
        self.assertEqual(res.json()["error_msg"], "Schema with that name already exists")

    def test_09_create_template_404(self):
        self.log.info("\n")
        self.log.info("09 test create_template_404".center(80, '-'))
        dataset_geid = self.dataset["global_entity_id"]
        payload = {
            "name": "unittestdataset2",
            "dataset_geid": dataset_geid,
            "tpl_geid": "notfound",
            "standard": "default",
            "system_defined": True,
            "is_draft": True,
            "content": {},
            "creator": "admin",
            "activity": [],
        }
        res = self.app.post(f"/v1/schema", json=payload)
        self.assertEqual(res.status_code, 400)
        self.assertEqual(res.json()["error_msg"], "Template not found")
