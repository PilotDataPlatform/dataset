import os
from pydantic import BaseSettings, Extra
from typing import Dict, Any

from common import VaultClient
from starlette.config import Config

config = Config(".env")
SRV_NAMESPACE = config("APP_NAME", cast=str, default="service_dataset")
CONFIG_CENTER_ENABLED = config("CONFIG_CENTER_ENABLED", cast=str, default="false")
CONFIG_CENTER_BASE_URL = config("CONFIG_CENTER_BASE_URL", cast=str, default="NOT_SET")


def load_vault_settings(settings: BaseSettings) -> Dict[str, Any]:
    if CONFIG_CENTER_ENABLED == 'false':
        return {}
    else:
        vc = VaultClient(config("VAULT_URL"), config("VAULT_CRT"), config("VAULT_TOKEN"))
        return vc.get_from_vault(SRV_NAMESPACE)

class Settings(BaseSettings):
    port: int = 5081
    host: str = "0.0.0.0"
    env: str = ""
    VERSION: str = '0.2.3'
    namespace: str = ""
    OPEN_TELEMETRY_ENABLED: str

    DATASET_FILE_FOLDER: str = "data"
    DATASET_SCHEMA_FOLDER: str = "schema"

    DATASET_CODE_REGEX: str = "^[a-z0-9]{3,32}$"

    # disk mounts
    ROOT_PATH: str

    CORE_ZONE_LABEL: str
    GREEN_ZONE_LABEL: str

    # minio
    MINIO_OPENID_CLIENT: str 
    MINIO_ENDPOINT: str
    MINIO_HTTPS: bool = False
    KEYCLOAK_URL: str
    MINIO_ACCESS_KEY: str
    MINIO_SECRET_KEY: str
    KEYCLOAK_MINIO_SECRET: str

    NEO4J_SERVICE: str
    QUEUE_SERVICE: str
    CATALOGUING_SERVICE: str
    ENTITYINFO_SERVICE: str
    ELASTIC_SEARCH_SERVICE: str
    UTILITY_SERVICE: str
    gm_queue_endpoint: str
    gm_username: str
    gm_password: str
    DATA_OPS_UTIL: str
    SEND_MESSAGE_URL: str

    RDS_HOST: str
    RDS_PORT: str
    RDS_DBNAME: str
    RDS_USER: str
    RDS_PWD: str
    RDS_SCHEMA_DEFAULT: str

    # Redis Service
    REDIS_HOST: str
    REDIS_PORT: str
    REDIS_DB: str
    REDIS_PASSWORD: str

    # download secret
    DOWNLOAD_KEY: str = "indoc101"
    DOWNLOAD_TOKEN_EXPIRE_AT: int = 5

    MAX_PREVIEW_SIZE: int = 500000

    # dataset schema default
    ESSENTIALS_NAME: str = "essential.schema.json"
    ESSENTIALS_TPL_NAME: str = "Essential"


    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'
        extra = Extra.allow

        @classmethod
        def customise_sources(
            cls,
            init_settings,
            env_settings,
            file_secret_settings,
        ):
            return (
                env_settings,
                load_vault_settings,
                init_settings,
                file_secret_settings,
            )

    def __init__(self) -> None:
        super().__init__()

        
        self.disk_namespace = self.namespace
        self.opentelemetry_enabled = self.OPEN_TELEMETRY_ENABLED == "TRUE"

        self.MINIO_TMP_PATH = self.ROOT_PATH + '/tmp/'

        self.NEO4J_SERVICE_V2 = self.NEO4J_SERVICE + "/v2/neo4j/"
        self.NEO4J_SERVICE += "/v1/neo4j/"
        self.QUEUE_SERVICE += "/v1/"
        self.CATALOGUING_SERVICE_V1 = self.CATALOGUING_SERVICE + "/v1/"
        self.CATALOGUING_SERVICE_V2 = self.CATALOGUING_SERVICE + "/v2/"
        self.COMMON_SERVICE = self.UTILITY_SERVICE + "/v1/"
        self.ENTITYINFO_SERVICE += "/v1/"
        self.ELASTIC_SEARCH_SERVICE += "/"
        
        self.DATA_UTILITY_SERVICE = self.DATA_OPS_UTIL + "/v1/"
        self.DATA_UTILITY_SERVICE_v2 = self.DATA_OPS_UTIL + "/v2/"
        self.SEND_MESSAGE_URL += "/v1/send_message"

        self.OPS_DB_URI = f"postgresql://{self.RDS_USER}:{self.RDS_PWD}@{self.RDS_HOST}/{self.RDS_DBNAME}"

        # Redis Service
        self.REDIS_PORT = int(self.REDIS_PORT)
        self.REDIS_DB = int(self.REDIS_DB)


ConfigClass = Settings()

