import unittest
from tests.logger import Logger
from tests.prepare_test import SetupTest

class TestCreateDataset(unittest.TestCase):
    log = Logger(name='test_create_dataset.log')
    test = SetupTest(log)

    @classmethod
    def setUpClass(cls):
        cls.log = cls.test.log
        cls.app = cls.test.app
        cls.payload = {
            "username": "amyguindoc14",
            "title": "123",
            "authors": [
                "123"
            ],
            "type": "GENERAL",
            "description": "123"
        }

    def test_01_create_dataset_with_shorten_code(self):
        self.log.info("\n")
        self.log.info("01 test create dataset with code less than 3".center(80, '-'))
        self.payload["code"] = "ot"
        res = self.app.post(f"/v1/dataset", json=self.payload)
        self.log.info(f"Response payload: {res}")
        self.assertEqual(res.status_code, 400)
        self.assertEqual(res.json()["error_msg"], "Invalid code")

    def test_02_create_dataset_with_longer_code(self):
        self.log.info("\n")
        self.log.info("02 test create dataset with code more than 32".center(80, '-'))
        self.payload["code"] = "ascbdascbdascbdascbdascbdascbda12"
        res = self.app.post(f"/v1/dataset", json=self.payload)
        self.log.info(f"Response payload: {res}")
        self.assertEqual(res.status_code, 400)
        self.assertEqual(res.json()["error_msg"], "Invalid code")

    def test_03_create_dataset_with_code_contain_space(self):
        self.log.info("\n")
        self.log.info("03 test create dataset with code contain space".center(80, '-'))
        self.payload["code"] = "p s"
        res = self.app.post(f"/v1/dataset", json=self.payload)
        self.log.info(f"Response payload: {res}")
        self.assertEqual(res.status_code, 400)
        self.assertEqual(res.json()["error_msg"], "Invalid code")

    def test_04_create_dataset_with_special_characters_code(self):
        self.log.info("\n")
        self.log.info("04 test create dataset with code contain special characters".center(80, '-'))
        self.payload["code"] = "ps!@#"
        res = self.app.post(f"/v1/dataset", json=self.payload)
        self.log.info(f"Response payload: {res}")
        self.assertEqual(res.status_code, 400)
        self.assertEqual(res.json()["error_msg"], "Invalid code")

    def test_05_create_dataset_with_empty_code(self):
        self.log.info("\n")
        self.log.info("05 test create dataset with code contain only space".center(80, '-'))
        self.payload["code"] = " "
        res = self.app.post(f"/v1/dataset", json=self.payload)
        self.log.info(f"Response payload: {res}")
        self.assertEqual(res.status_code, 400)
        self.assertEqual(res.json()["error_msg"], "Invalid code")