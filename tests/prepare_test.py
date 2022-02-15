import requests
from app.config import ConfigClass
from fastapi.testclient import TestClient
from run import app


class SetupException(Exception):
    "Failed setup test"


def get_geid():
    '''
    get geid
    http://10.3.7.222:5062/v1/utility/id?entity_type=data_upload
    '''
    url = ConfigClass.COMMON_SERVICE + \
        "utility/id"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['result']
    else:
        raise Exception('{}: {}'.format(response.status_code, url))


class SetupTest:

    object_id_list = []

    def __init__(self, log):
        print("prepare the dataset file test")
        self.log = log
        self.app = self.create_test_client()

    def create_test_client(self):
        client = TestClient(app)
        return client

    
    def create_test_project_with_data(self, project_name):
        '''
        The structure will be:
        Project_<time>
            |--- File 1
            |--- Folder 1
                    |--- File 2
        '''

        test_payload = {
            "name": project_name,
            "path": project_name,
            "code": project_name,
            "description": "Project created by unit test, will be deleted soon...",
            "discoverable": 'true',
            "type": "Usecase",
            "tags": ['test'],
            "global_entity_id": get_geid(),
        }

        test_project_node = requests.post(ConfigClass.NEO4J_SERVICE + "nodes/Container", json=test_payload)
        test_project_node = test_project_node.json()[0]

        # create a name folder
        test_payload = {
            "archived":False,
            "display_path":"",
            "folder_level":0,
            "folder_relative_path":"",
            "global_entity_id":get_geid(),
            "list_priority":10,
            "name":"admin",
            "project_code":project_name,
            "uploader":"admin",
        }
        test_name_node = requests.post(ConfigClass.NEO4J_SERVICE + "nodes/Folder", json=test_payload)
        test_name_node = test_name_node.json()[0]

        # create a folder
        test_payload = {
            "archived":False,
            "display_path":"admin/folder1",
            "folder_level":1,
            "folder_relative_path":"admin",
            "global_entity_id":get_geid(),
            "list_priority":10,
            "name":"folder1",
            "project_code":project_name,
            "uploader":"admin",
        }
        test_folder_node = requests.post(ConfigClass.NEO4J_SERVICE + "nodes/Folder", json=test_payload)
        test_folder_node = test_folder_node.json()[0]
        

        # create two file
        test_payload = {
            "archived":False,
            "display_path":"admin/folder1/file1",
            "file_size": 1,
            "global_entity_id":get_geid(),
            "list_priority": 20,
            "location": "minio://http://10.3.7.220/gr-jun29test/admin/hello123/Android.svg",
            "name": "file1",
            "operator":"admin",
            "project_code": project_name,
            "uploader": "admin",
        }
        test_file_node_1 = requests.post(ConfigClass.NEO4J_SERVICE + "nodes/File", json=test_payload)
        test_file_node_1 = test_file_node_1.json()[0]
        
        test_payload = {
            "archived":False,
            "display_path":"admin/file2",
            "file_size": 1,
            "global_entity_id":get_geid(),
            "list_priority": 10,
            "location": "minio://http://10.3.7.220/gr-jun29test/admin/hello123/Android.svg",
            "name": "file2",
            "operator":"admin",
            "project_code": project_name,
            "uploader": "admin",
        }
        test_file_node_2 = requests.post(ConfigClass.NEO4J_SERVICE + "nodes/File", json=test_payload)
        test_file_node_2 = test_file_node_2.json()[0]

        # make order as project->folder1->file1->file2
        self.__class__.object_id_list.append(test_project_node.get("id"))
        self.__class__.object_id_list.append(test_name_node.get("id"))
        self.__class__.object_id_list.append(test_folder_node.get("id"))
        self.__class__.object_id_list.append(test_file_node_1.get("id"))
        self.__class__.object_id_list.append(test_file_node_2.get("id"))

        print(self.object_id_list)

        # make the relationship
        create_node_url = ConfigClass.NEO4J_SERVICE + 'relations/own'
        new_relation = requests.post(create_node_url, json={
            "start_id":self.object_id_list[0], "end_id":self.object_id_list[4]})
        new_relation = requests.post(create_node_url, json={
            "start_id":self.object_id_list[2], "end_id":self.object_id_list[3]})
        new_relation = requests.post(create_node_url, json={
            "start_id":self.object_id_list[1], "end_id":self.object_id_list[2]})
        new_relation = requests.post(create_node_url, json={
            "start_id":self.object_id_list[0], "end_id":self.object_id_list[1]})

        return test_project_node.get("global_entity_id"), \
            [
                test_folder_node.get("global_entity_id"),
                test_file_node_2.get("global_entity_id")
            ]


    def create_test_dataset(self, dataset_name):
        payload = {
            "username": "admin",
            "title": dataset_name,
            "code": dataset_name,
            "authors": ["list"],
            "type": "GENERAL",
            "modality": [],
            "collection_metohd": ["test"],
            "license": "str",
            "tags": ["list"],
            "description": "str",
        }
        res = self.app.post("/v1/dataset", json=payload)
        return res.json().get("result", {})

    def add_file_to_dataset(self, file_id, dataset_id):
        payload = {
            "start_id": dataset_id,
            "end_id": dataset_id,
        }
        response = requests.post(ConfigClass.NEO4J_SERVICE + "relations/own", json=payload)
        self.log.info(f"Create relation response: {response.text}")


####################################################################################################

    def delete_test_project_and_files(self, project_name):
        # delete relationship
        relation_delete_url = ConfigClass.NEO4J_SERVICE + "relations"
        _ = requests.delete(relation_delete_url, json={
            "start_id":self.object_id_list[0], "end_id":self.object_id_list[4]})
        _ = requests.delete(relation_delete_url, json={
            "start_id":self.object_id_list[2], "end_id":self.object_id_list[3]})
        _ = requests.delete(relation_delete_url, json={
            "start_id":self.object_id_list[1], "end_id":self.object_id_list[2]})
        _ = requests.delete(relation_delete_url, json={
            "start_id":self.object_id_list[0], "end_id":self.object_id_list[1]})

        # then remove the nodes
        node_delete_url = ConfigClass.NEO4J_SERVICE + "nodes/Container/node/%s"%(self.object_id_list[0])
        response = requests.delete(node_delete_url)
        node_delete_url = ConfigClass.NEO4J_SERVICE + "nodes/Folder/node/%s"%(self.object_id_list[1])
        response = requests.delete(node_delete_url)
        node_delete_url = ConfigClass.NEO4J_SERVICE + "nodes/Folder/node/%s"%(self.object_id_list[2])
        response = requests.delete(node_delete_url)
        node_delete_url = ConfigClass.NEO4J_SERVICE + "nodes/File/node/%s"%(self.object_id_list[3])
        response = requests.delete(node_delete_url)
        node_delete_url = ConfigClass.NEO4J_SERVICE + "nodes/File/node/%s"%(self.object_id_list[4])
        response = requests.delete(node_delete_url)



    def delete_test_dataset(self, dataset_id):

        res = requests.delete(ConfigClass.NEO4J_SERVICE + "nodes/Dataset/node/%d"%(dataset_id))
        if res.status_code != 200:
            raise Exception(f"Error removing dataset: {res.json()}")

    def create_file(self, file_event):
        self.log.info("\n")
        self.log.info("Creating testing file".ljust(80, '-'))
        filename = file_event.get('filename')
        dataset_code = file_event.get('dataset_code')
        file_size = file_event.get("file_size", 1000)
        geid = file_event.get("global_entity_id")
        payload = {
            "name": filename,
            "operator": "admin",
            "location": f"minio://http://10.3.7.220/{dataset_code}/{filename}",
            "file_size": file_size,
            "dataset_code": dataset_code,
            "global_entity_id": geid,
        }
        try:
            res = requests.post(ConfigClass.NEO4J_SERVICE + "nodes/File", json=payload)
            self.log.info(f"RESPONSE DATA: {res.text}")
            self.log.info(f"RESPONSE STATUS: {res.status_code}")
            assert res.status_code == 200
            return res.json()[0] 
        except Exception as e:
            self.log.info(f"ERROR CREATING FILE: {e}")
            raise e

    def delete_file_node(self, node_id):
        self.log.info("\n")
        self.log.info("Preparing delete file node".ljust(80, '-'))
        delete_api = ConfigClass.NEO4J_SERVICE + "nodes/File/node/%s" % str(node_id)
        try:
            delete_res = requests.delete(delete_api)
            self.log.info(f"DELETE STATUS: {delete_res.status_code}")
            self.log.info(f"DELETE RESPONSE: {delete_res.text}")
        except Exception as e:
            self.log.info(f"ERROR DELETING FILE: {e}")
            self.log.info(f"PLEASE DELETE THE FILE MANUALLY WITH ID: {node_id}")
            raise e



    def clean_up_test_files(self, test_dataset_geid, ff_list):

        payload = {
            "source_list":[ff.get("global_entity_id") for ff in ff_list], 
            "operator":"admin"
        }
        res = self.app.delete("/v1/dataset/%s/files"%(test_dataset_geid), json=payload)

    def create_schema_template(self, dataset_geid):
        payload = {
            "name": "unittestschematemplate",
            "standard": "test",
            "system_defined": True,
            "is_draft": True,
            "content": {},
            "creator": "admin", 
        }
        res = self.app.post(f"/v1/dataset/{dataset_geid}/schemaTPL", json=payload)
        self.log.info(res.json())
        return res.json()["result"]

    def delete_schema_template(self, dataset_geid, template_geid):
        res = self.app.delete(f"/v1/dataset/{dataset_geid}/schemaTPL/{template_geid}")
        self.log.info(res.json())
        return res.json()

    def delete_schema(self, dataset_geid, template_geid):
        payload = {
            "username": "admin",
            "dataset_geid": dataset_geid,
            "activity": []
        }
        res = self.app.delete(f"/v1/schema/{template_geid}", json=payload)
        self.log.info(res.json())
        return res.json()
