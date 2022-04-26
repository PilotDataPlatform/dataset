# Copyright (C) 2022 Indoc Research
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import httpx
from minio import Minio
from minio.commonconfig import CopySource
from minio.credentials.providers import ClientGrantsProvider

from app.config import ConfigClass


class Minio_Client_:
    def __init__(self, access_token, refresh_token):
        # preset the tokens for refreshing
        self.access_token = access_token
        self.refresh_token = refresh_token

        # retrieve credential provide with tokens
        c = self.get_provider()

        self.client = Minio(ConfigClass.MINIO_ENDPOINT, credentials=c, secure=ConfigClass.MINIO_HTTPS)

    # function helps to get new token/refresh the token
    def _get_jwt(self):
        # enable the token exchange with different azp
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        payload = {
            'grant_type': 'urn:ietf:params:oauth:grant-type:token-exchange',
            'subject_token': self.access_token.replace('Bearer ', ''),
            'subject_token_type': 'urn:ietf:params:oauth:token-type:access_token',
            'requested_token_type': 'urn:ietf:params:oauth:token-type:refresh_token',
            'client_id': 'minio',
            'client_secret': ConfigClass.KEYCLOAK_MINIO_SECRET,
        }

        # use http request to fetch from keycloak
        with httpx.Client() as client:
            result = client.post(ConfigClass.KEYCLOAK_URL, data=payload, headers=headers)
        if result.status_code != 200:
            raise Exception('Token refresh failed with ' + str(result.json()))

        self.access_token = result.json().get('access_token')
        self.refresh_token = result.json().get('refresh_token')

        jwt_object = result.json()
        # print(jwt_object)

        return jwt_object

    # use the function above to create a credential object in minio
    # it will use the jwt function to refresh token if token expired
    def get_provider(self):
        minio_http = ('https://' if ConfigClass.MINIO_HTTPS else 'http://') + ConfigClass.MINIO_ENDPOINT
        # print(minio_http)
        provider = ClientGrantsProvider(
            self._get_jwt,
            minio_http,
        )

        return provider

    def copy_object(self, bucket, obj, source_bucket, source_obj):
        result = self.client.copy_object(
            bucket,
            obj,
            CopySource(source_bucket, source_obj),
        )
        return result

    def delete_object(self, bucket, obj):
        result = self.client.remove_object(bucket, obj)
        return result

    # this will first call the copy api and delete the source
    def move_object(self, bucket, obj, source_bucket, source_obj):
        result = self.copy_object(bucket, obj, source_bucket, source_obj)
        result = self.delete_object(source_bucket, source_obj)
        return result


class Minio_Client:
    def __init__(self):

        # Temperary use the credential
        self.client = Minio(
            ConfigClass.MINIO_ENDPOINT,
            access_key=ConfigClass.MINIO_ACCESS_KEY,
            secret_key=ConfigClass.MINIO_SECRET_KEY,
            secure=ConfigClass.MINIO_HTTPS,
        )

    def copy_object(self, bucket, obj, source_bucket, source_obj):
        result = self.client.copy_object(
            bucket,
            obj,
            CopySource(source_bucket, source_obj),
        )
        return result

    def delete_object(self, bucket, obj):
        result = self.client.remove_object(bucket, obj)
        return result

    # this will first call the copy api and delete the source
    def move_object(self, bucket, obj, source_bucket, source_obj):
        result = self.copy_object(bucket, obj, source_bucket, source_obj)
        result = self.delete_object(source_bucket, source_obj)
        return result
