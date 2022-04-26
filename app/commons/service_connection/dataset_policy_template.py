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

import uuid


# admin will have all the permission to access
# all the name folder under the bucket
def create_dataset_policy_template(dataset_code, content=None):
    if not content:
        content = '''
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": [
                        "s3:ListBucket",
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:GetBucketLocation",
                        "s3:DeleteObject"],
                    "Effect": "Allow",
                    "Resource": ["arn:aws:s3:::%s"]
                }
            ]
        }
        ''' % (
            dataset_code
        )

    # now create the template file since we need to use the file
    # with minio admin client to create policy
    # since here we will write to disk. to avoid collision use the uuid4
    template_name = str(uuid.uuid4()) + '.json'
    policy_file = open(template_name, 'w')
    policy_file.write(content)
    policy_file.close()

    return template_name
