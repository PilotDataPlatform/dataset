import unittest
from tests.prepare_test import SetupTest
from tests.logger import Logger
import shutil
from app.config import ConfigClass
import os
import uuid
import time
import requests

class TestDatasetFileAPI(unittest.TestCase):
    log = Logger(name='test_dataset_file_api.log')
    test = SetupTest(log)
    test_dataset = "testdataset%d"%int(time.time())
    test_dataset_id = -1
    test_dataset_geid = None
    test_project = "testproject%d"%int(time.time())

    # test_dataset = "testdataset1626104649"
    # test_dataset_id = 18590
    # test_dataset_geid = "9ff8382d-f476-4cdf-a357-66c4babf8320-1626104650"

    # source sample
    test_source_project = "5baeb6a1-559b-4483-aadf-ef60519584f3-1620404058"
    test_source_list = [
        "b1064aa6-edbe-4eb6-b560-a8552f2f6162-1626719078",
        "b2411265-a207-499d-b51c-8eb4d1937450-1628620874", 
        "af8c7e7e-8a78-4dbc-8cdb-97ca1c985172-1626709871"
    ]

    files = []

    @classmethod
    def setUpClass(cls):
        cls.log = cls.test.log
        cls.app = cls.test.app

        res = cls.test.create_test_dataset(cls.test_dataset)
        cls.test_dataset_id = res.get("id")
        cls.test_dataset_geid = res.get("global_entity_id")
        print(cls.test_dataset_id)

        # cls.test_source_project, cls.test_source_list = \
        #     cls.test.create_test_project_with_data(cls.test_project)


    @classmethod
    def tearDownClass(cls):
        cls.log.info("\n")
        cls.log.info("START TEAR DOWN PROCESS")
        try:

            cls.test.clean_up_test_files(cls.test_dataset_geid, cls.files)
            # cls.test.delete_test_project_and_files(cls.test_project)
            cls.test.delete_test_dataset(cls.test_dataset_id)
            cls.log.info("Deleting folder path on tear down")
        except Exception as e:
            cls.log.error("Please manual delete node and entity")
            cls.log.error(e)
            raise e


    def test_01_import_from_project(self):
        '''
        TEST 01: try to import files from a source list
        Expected Result: api return 200 with empty error
        '''

        payload = {
            "source_list": self.test_source_list,
            "operator":"admin", 
            "project_geid": self.test_source_project,
        }
        res = self.app.put("/v1/dataset/%s/files"%(self.test_dataset_geid), json=payload)
        # print(res.__dict__)
        self.assertEqual(res.status_code, 200)

        return

    def test_02_list_files_from_dataset(self):
        '''
        TEST 02: try to list the file we just import from test 01
        Expected Result: api return 200 with files = len(test_source_list)
        '''

        res = self.app.get("/v1/dataset/%s/files"%(self.test_dataset_geid))
        self.assertEqual(res.status_code, 200)

        self.__class__.files = res.json().get("result").get("data")
        # print(res.json())
        self.assertEqual(len(self.files), len(self.test_source_list))

        return


    def test_03_test_import_from_403(self):
        '''
        TEST 03: try to import files from a different project
                 at this time, we dont allow user to import from
                 different project once we import before
        Expected Result: api return 403 with block message
        '''

        # get random project
        res = requests.post(ConfigClass.NEO4J_SERVICE + "nodes/Container/query", 
            json={"limit": 1})
        different_project = res.json()[0]

        # try to import from different project
        # this time should be 403
        payload = {
            "source_list": [],
            "operator":"admin", 
            "project_geid": different_project.get("global_entity_id"),
        }
        res = self.app.put("/v1/dataset/%s/files"%(self.test_dataset_geid), json=payload)
        self.assertEqual(res.status_code, 403)


    def test_04_test_import_from_404(self):
        '''
        TEST 04: try to import files from a non-existing project
        Expected Result: api return 404
        '''

        # try to import into dataset that does not exists
        # this time should be 404
        payload = {
            "source_list": [],
            "operator":"admin", 
            "project_geid": "NOT_EXIST_Project",
        }
        res = self.app.put("/v1/dataset/%s/files"%("NOT_EXIST_Dataset"), json=payload)
        self.assertEqual(res.status_code, 404)


    def test_05_test_import_duplicate(self):
        payload = {
            "source_list": self.test_source_list,
            "operator":"admin", 
            "project_geid": self.test_source_project,
        }
        res = self.app.put("/v1/dataset/%s/files"%(self.test_dataset_geid), json=payload)
        self.assertEqual(res.status_code, 200)
        ignored_file = [x.get("global_entity_id") for x in res.json().get("result").get("ignored")]
        self.assertEqual(ignored_file, self.test_source_list)

        return

    
    # #######################################################################################
    
    def test_10_move_to_subfolder(self):
        print(self.files)

        # pick the folder
        file_geid, folder_geid = None, None
        for x in self.files:
            if "File" in x.get("labels", []):
                file_geid = x.get("global_entity_id")
            elif "Folder" in x.get("labels", []):
                folder_geid = x.get("global_entity_id")

        payload = {
            "source_list": [file_geid], 
            "operator":"admin",
            "target_geid": folder_geid
        }
        res = self.app.post("/v1/dataset/%s/files"%(self.test_dataset_geid), json=payload)
        print(res.__dict__)
        self.assertEqual(res.status_code, 200)
        processing_file = [x.get("global_entity_id") for x in res.json().get("result").get("processing")]
        self.assertEqual(processing_file, [file_geid])

        # then list file again to see if the file has been deleted
        res = self.app.get("/v1/dataset/%s/files"%(self.test_dataset_geid))
        files = res.json().get("result").get("data")
        self.assertEqual(len(files), len(self.test_source_list) - 1)


    # def test_11_move_back_to_root(self):
    #     payload = {
    #         "source_list": [self.files[0].get("global_entity_id")], 
    #         "operator":"admin",
    #         "target_geid": self.test_dataset_geid
    #     }

    #     res = self.app.post("/v1/dataset/%s/files"%(self.test_dataset_geid), json=payload)
    #     self.assertEqual(res.status_code, 200)
    #     processing_file = [x.get("global_entity_id") for x in res.json().get("result").get("processing")]
    #     self.assertEqual(processing_file, [self.files[0].get("global_entity_id")])

    #     # then list file again to see if the file has been deleted
    #     res = self.app.get("/v1/dataset/%s/files"%(self.test_dataset_geid))
    #     files = res.json().get("result").get("data")
    #     self.assertEqual(len(files), len(self.test_source_list))

    
    def test_12_move_wrong_file(self):

        # get a random file
        res = requests.post(ConfigClass.NEO4J_SERVICE + "nodes/Container/query", 
            json={"limit": 1})
        random_file = res.json()[0]

        payload = {
            "source_list": [random_file.get("global_entity_id")], 
            "operator":"admin",
            "target_geid": self.test_dataset_geid
        }

        res = self.app.post("/v1/dataset/%s/files"%(self.test_dataset_geid), json=payload)
        self.assertEqual(res.status_code, 200)
        ignored_file = [x.get("global_entity_id") for x in res.json().get("result").get("ignored")]
        self.assertEqual(ignored_file, [random_file.get("global_entity_id")])

        # should be nothing happened
        res = self.app.get("/v1/dataset/%s/files"%(self.test_dataset_geid))
        files = res.json().get("result").get("data")
        self.assertEqual(len(files), len(self.test_source_list)-1)

    ####################################################################################################

    def test_20_delete_from_dataset(self):
        '''
        TEST 20: try to delete a file that under dataset
        Expected Result: the file is deleted -> length -1
        '''

        res = self.app.get("/v1/dataset/%s/files"%(self.test_dataset_geid))
        self.__class__.files = res.json().get("result").get("data")

        payload = {
            "source_list":[self.files[0].get("global_entity_id")], 
            "operator":"admin"
        }
        res = self.app.delete("/v1/dataset/%s/files"%(self.test_dataset_geid), json=payload)
        self.assertEqual(res.status_code, 200)
        processing_file = [x.get("global_entity_id") for x in res.json().get("result").get("processing")]
        self.assertEqual(processing_file, [self.files[0].get("global_entity_id")])

        # then list file again to see if the file has been deleted
        res = self.app.get("/v1/dataset/%s/files"%(self.test_dataset_geid))
        files = res.json().get("result").get("data")
        self.assertEqual(len(files), len(self.test_source_list) - 2)

        return

    def test_21_delete_from_dataset_with_random_file(self):
        '''
        TEST 21: try to delete a file that is NOT belongs to dataset
        Expected Result: nothing happened
        '''

        # get random file
        res = requests.post(ConfigClass.NEO4J_SERVICE + "nodes/Container/query", 
            json={"limit": 1})
        random_file = res.json()[0]

        payload = {
            "source_list":[random_file.get("global_entity_id")], 
            "operator":"admin"
        }

        res = self.app.delete("/v1/dataset/%s/files"%(self.test_dataset_geid), json=payload)
        self.assertEqual(res.status_code, 200)
        ignored_file = [x.get("global_entity_id") for x in res.json().get("result").get("ignored")]
        self.assertEqual(ignored_file, [random_file.get("global_entity_id")])

        # then list file again this time should be nothing happened
        res = self.app.get("/v1/dataset/%s/files"%(self.test_dataset_geid))
        files = res.json().get("result").get("data")
        self.assertEqual(len(files), len(self.test_source_list) - 2)

        return




    

