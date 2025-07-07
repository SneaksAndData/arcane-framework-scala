#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import pandas as pd
import string
import random
import os
import boto3

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    _ = s3_client.upload_file(file_name, bucket, object_name)


def generate_parquet_file(fname):
    def _generate_value(index):
        if index % 2 == 0:
            return random.randint(1, 100)

        alphanum = list(string.ascii_uppercase + string.digits)
        random.shuffle(alphanum)
        return ''.join(alphanum[:random.randint(1, 10)])


    data = { f"col{i}": [_generate_value(i) for _ in range(100)] for i in range(10)}

    df = pd.DataFrame(data=data)
    df.to_parquet(f'{fname}.parquet.gzip', compression='gzip')

def generate_parquet_test_files():
    os.makedirs("/tmp/s3-parquet", exist_ok=True)

    for ix_file in range(50):
        generate_parquet_file(f'/tmp/s3-parquet/{ix_file}')
        upload_file(f'/tmp/s3-parquet/{ix_file}.parquet.gzip', 's3-blob-reader')

generate_parquet_test_files()
