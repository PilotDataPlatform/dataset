from app.config import ConfigClass
import requests
import time


def get_geid():
    url = ConfigClass.COMMON_SERVICE + "utility/id"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['result']
    else:
        raise Exception('get_geid {}: {}'.format(response.status_code, url))

