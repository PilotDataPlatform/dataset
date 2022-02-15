import unittest
from tests.prepare_test import SetupTest
from tests.logger import Logger


def setUpModule():
    pass


def tearDownModule():
    pass


class TestAPI(unittest.TestCase):
    log = Logger(name='test_api.log')
    test = SetupTest(log)
    # app = test.client

    @classmethod
    def setUpClass(cls) -> None:
        pass

    @classmethod
    def tearDownClass(cls) -> None:
        pass

    def setUp(self) -> None:
        pass

    def tearDown(self) -> None:
        pass

    def test_01_api(self):
        self.log.info("test case 1")



