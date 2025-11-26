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

import json
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


def generate_value(index):
    if index % 2 == 0:
        return random.randint(1, 100)

    alphanum = list(string.ascii_uppercase + string.digits)
    random.shuffle(alphanum)
    return ''.join(alphanum[:random.randint(1, 10)])

def get_parquet_test_data():
    return { f"col{i}": [generate_value(i) for _ in range(100)] for i in range(10)}

def get_json_test_data():
    return { "index": random.randint(0, 1000), "body": { f"col{i}": generate_value(i) for i in range(10) } }

def get_json_test_data_variable_content():
    return { "index": random.randint(0, 1000), "body": { f"col{i}": generate_value(i) for i in range(10) if i % random.randint(1, 9) == 0 } }

def get_json_test_data_nested_array():
    base_body = { f"col{i}": generate_value(i) for i in range(10) if i % random.randint(1, 9) == 0 }
    full_body = base_body | { "nested_array": { "value": [{ f"nested_col_1": generate_value(1), f"nested_col_2": generate_value(2) } for i in range(10)] } }
    return { "index": random.randint(0, 1000), "body": full_body }

def generate_parquet_file(fname):
    df = pd.DataFrame(data=get_parquet_test_data())
    df.to_parquet(f'{fname}.parquet.gzip', compression='gzip')

def generate_json_file(fname, line_count, generator_func):
    content = ""
    json_lines = [json.dumps(generator_func()) for i in range(line_count)]
    with open(f'{fname}.json', 'w', encoding='utf-8') as f:
        f.write('\n'.join(json_lines))

def generate_parquet_test_files():
    os.makedirs("/tmp/s3-parquet", exist_ok=True)

    for ix_file in range(50):
        generate_parquet_file(f'/tmp/s3-parquet/{ix_file}')
        upload_file(f'/tmp/s3-parquet/{ix_file}.parquet.gzip', 's3-blob-reader')

def generate_json_test_files():
    os.makedirs("/tmp/s3-json", exist_ok=True)
    os.makedirs("/tmp/s3-json-variable", exist_ok=True)
    os.makedirs("/tmp/s3-json-nested-array", exist_ok=True)

    for ix_file in range(50):
        generate_json_file(f'/tmp/s3-json/{ix_file}', 100, get_json_test_data)
        generate_json_file(f'/tmp/s3-json-variable/{ix_file}', 100, get_json_test_data_variable_content)
        generate_json_file(f'/tmp/s3-json-nested-array/{ix_file}', 10, get_json_test_data_nested_array)

        upload_file(f'/tmp/s3-json/{ix_file}.json', 's3-blob-reader-json')
        upload_file(f'/tmp/s3-json-variable/{ix_file}.json', 's3-blob-reader-json-variable')
        upload_file(f'/tmp/s3-json-nested-array/{ix_file}.json', 's3-blob-reader-json-nested-array')

generate_parquet_test_files()
generate_json_test_files()
