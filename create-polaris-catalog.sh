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

# https://github.com/apache/polaris/blob/45602456edb139c41553fb37716c151db597608b/getting-started/trino/create-polaris-catalog.sh

PRINCIPAL_TOKEN="principal:root;realm:default-realm"

# Use s3 filesystem by default
curl -i -X POST -H "Authorization: Bearer $PRINCIPAL_TOKEN" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://10.1.0.4:8181/api/management/v1/catalogs \
  -d '{
        "catalog": {
          "name": "polaris",
          "type": "INTERNAL",
          "readOnly": false,
          "properties": {
            "default-base-location": "s3://tmp/polaris/"
          },
          "storageConfigInfo": {
            "storageType": "S3",
            "allowedLocations": [
              "s3://tmp/polaris/",
              "s3://lakehouse/polaris/"
            ],
            "roleArn": "arn:aws:iam::000000000000:role/polaris-access-role"
          }
        }
      }'
# create namespace
curl -i -X POST -H "Authorization: Bearer $PRINCIPAL_TOKEN" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://10.1.0.4:8181/api/catalog/v1/polaris/namespaces \
  -d '{
        "namespace": [
          "test"
        ],
        "properties": {}
      }'

# create principal role
curl -i -X POST -H "Authorization: Bearer $PRINCIPAL_TOKEN" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://10.1.0.4:8181/api/management/v1/principal-roles \
  -d '{
        "principalRole": {
          "name": "admin"
        }
      }'

# create catalog role
curl -i -X POST -H "Authorization: Bearer $PRINCIPAL_TOKEN" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://10.1.0.4:8181/api/management/v1/catalogs/polaris/catalog-roles \
  -d '{
        "catalogRole": {
          "name": "admin"
        }
      }'

# assign principal role
curl -i -X PUT -H "Authorization: Bearer $PRINCIPAL_TOKEN" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://10.1.0.4:8181/api/management/v1/principals/root/principal-roles \
  -d '{
        "principalRole": {
          "name": "admin"
        }
      }'

# assign principal role to catalog role
curl -i -X PUT -H "Authorization: Bearer $PRINCIPAL_TOKEN" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://10.1.0.4:8181/api/management/v1/principal-roles/admin/catalog-roles/polaris \
  -d '{
        "catalogRole": {
          "name": "admin"
        }
      }'

# add grant to catalog role
curl -i -X PUT -H "Authorization: Bearer $PRINCIPAL_TOKEN" -H 'Accept: application/json' -H 'Content-Type: application/json' \
  http://10.1.0.4:8181/api/management/v1/catalogs/polaris/catalog-roles/admin/grants \
  -d '{
        "grant": {
          "type": "catalog",
          "privilege": "CATALOG_MANAGE_CONTENT"
        }
      }'