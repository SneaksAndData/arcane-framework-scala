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

from azure.storage.blob import BlobServiceClient
from datetime import datetime, timedelta
import os

CONTENT = """50bff458-d47a-4924-804b-31c0a83108e6,"1/1/2020 0:00:00 PM","1/1/2020 0:00:00 PM",0,1111000000,1111000010,"F1234567",1,,"2020-01-01T00:15:00.0000000Z","acc1",111111110,"2020-01-01T00:15:00.0000000Z","acc1",0,"dat",1,1111000001,2111000001,1111000001,21111,2111000001,"2020-01-01T00:15:00.0000000+00:00","2020-01-01T00:15:00.0000000Z",
5b4bc74e-2132-4d8e-8572-48ce4260f182,"1/1/2020 0:00:01 PM","1/1/2020 0:00:01 PM",0,1111000001,1111000011,"F1234568",1,,"2020-01-01T00:16:00.0000000Z","acc2",111111111,"2020-01-01T00:16:00.0000000Z","acc2",0,"dat",1,1111000002,2111000002,1111000001,21111,2111000001,"2020-01-01T00:16:00.0000000+00:00","2020-01-01T00:16:00.0000000Z",
aae2094d-cd17-42b4-891e-3b268e2b6713,"1/1/2020 0:00:01 PM","1/1/2020 0:00:01 PM",0,1111000002,1111000012,"F1234569",1,,"2020-01-01T00:17:00.0000000Z","acc2",111111112,"2020-01-01T00:17:00.0000000Z","acc2",0,"dat",1,1111000003,2111000003,1111000001,21111,2111000001,"2020-01-01T00:17:00.0000000+00:00","2020-01-01T00:17:00.0000000Z",
9633be9a-c485-4afa-8bb7-4ba380eaa206,"1/1/2020 0:00:01 PM","1/1/2020 0:00:01 PM",0,1111000003,1111000013,"F1234578",1,,"2020-01-01T00:18:00.0000000Z","acc1",111111113,"2020-01-01T00:18:00.0000000Z","acc1",0,"dat",1,1111000004,2111000004,1111000001,21111,2111000001,"2020-01-01T00:18:00.0000000+00:00","2020-01-01T00:18:00.0000000Z",
b62c7b67-b8f8-4635-8cef-1c23591d5c4c,"1/1/2020 0:00:01 PM","1/1/2020 0:00:01 PM",0,1111000004,1111000014,"F1234511",1,,"2020-01-01T00:19:00.0000000Z","acc2",111111114,"2020-01-01T00:19:00.0000000Z","acc2",0,"dat",1,1111000005,2111000005,1111000001,21111,2111000001,"2020-01-01T00:19:00.0000000+00:00","2020-01-01T00:19:00.0000000Z",
"""
DELETE_FILE_CONTENT = """50bff458-d47a-4924-804b-31c0a83108e6,"1/1/2020 0:00:00 PM","1/1/2020 0:00:00 PM",,,,,,,,,,,,,,,,,,,3111000001,"0001-01-03T00:00:00.0000000",,True
"""

MODEL_JSON = """{
                  "name": "cdm",
                  "description": "cdm",
                  "version": "1.0",
                  "entities": [
                    {
                      "$type": "LocalEntity",
                      "name": "currency",
                      "description": "currency",
                      "annotations": [
                        {
                          "name": "Athena:PartitionGranularity",
                          "value": "Year"
                        },
                        {
                          "name": "Athena:InitialSyncState",
                          "value": "Completed"
                        },
                        {
                          "name": "Athena:InitialSyncDataCompletedTime",
                          "value": "1/1/2020 0:00:00 PM"
                        }
                      ],
                      "attributes": [
                        {
                          "name": "Id",
                          "dataType": "guid",
                          "maxLength": -1
                        },
                        {
                          "name": "SinkCreatedOn",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "SinkModifiedOn",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "iseuro",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "roundofftypeassetdep_jp",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "roundofftypeprice",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "roundofftypepurch",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "roundofftypesales",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "ltmroundofftypelineamount",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "sysdatastatecode",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "currencycode",
                          "dataType": "string",
                          "maxLength": 3,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 3
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "currencycodeiso",
                          "dataType": "string",
                          "maxLength": 3,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 3
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "roundingprecision",
                          "dataType": "decimal",
                          "maxLength": -1,
                          "cdm:traits": [
                            {
                              "traitReference": "is.dataFormat.numeric.shaped",
                              "arguments": [
                                {
                                  "name": "precision",
                                  "value": 38
                                },
                                {
                                  "name": "scale",
                                  "value": 6
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "roundoffassetdep_jp",
                          "dataType": "decimal",
                          "maxLength": -1,
                          "cdm:traits": [
                            {
                              "traitReference": "is.dataFormat.numeric.shaped",
                              "arguments": [
                                {
                                  "name": "precision",
                                  "value": 38
                                },
                                {
                                  "name": "scale",
                                  "value": 6
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "roundoffprice",
                          "dataType": "decimal",
                          "maxLength": -1,
                          "cdm:traits": [
                            {
                              "traitReference": "is.dataFormat.numeric.shaped",
                              "arguments": [
                                {
                                  "name": "precision",
                                  "value": 38
                                },
                                {
                                  "name": "scale",
                                  "value": 6
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "roundoffpurch",
                          "dataType": "decimal",
                          "maxLength": -1,
                          "cdm:traits": [
                            {
                              "traitReference": "is.dataFormat.numeric.shaped",
                              "arguments": [
                                {
                                  "name": "precision",
                                  "value": 38
                                },
                                {
                                  "name": "scale",
                                  "value": 6
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "roundoffsales",
                          "dataType": "decimal",
                          "maxLength": -1,
                          "cdm:traits": [
                            {
                              "traitReference": "is.dataFormat.numeric.shaped",
                              "arguments": [
                                {
                                  "name": "precision",
                                  "value": 38
                                },
                                {
                                  "name": "scale",
                                  "value": 6
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "symbol",
                          "dataType": "string",
                          "maxLength": 5,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 5
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "txt",
                          "dataType": "string",
                          "maxLength": 120,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 120
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "exchratemaxvariationpercent_mx",
                          "dataType": "decimal",
                          "maxLength": -1,
                          "cdm:traits": [
                            {
                              "traitReference": "is.dataFormat.numeric.shaped",
                              "arguments": [
                                {
                                  "name": "precision",
                                  "value": 38
                                },
                                {
                                  "name": "scale",
                                  "value": 6
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "decimalscount_mx",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "ltmroundofflineamount",
                          "dataType": "decimal",
                          "maxLength": -1,
                          "cdm:traits": [
                            {
                              "traitReference": "is.dataFormat.numeric.shaped",
                              "arguments": [
                                {
                                  "name": "precision",
                                  "value": 38
                                },
                                {
                                  "name": "scale",
                                  "value": 6
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "modifieddatetime",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "modifiedby",
                          "dataType": "string",
                          "maxLength": 20,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 20
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "modifiedtransactionid",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "createddatetime",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "createdby",
                          "dataType": "string",
                          "maxLength": 20,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 20
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "createdtransactionid",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "dataareaid",
                          "dataType": "string",
                          "maxLength": 4,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 4
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "recversion",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "partition",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "sysrowversion",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "recid",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "tableid",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "versionnumber",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "createdon",
                          "dataType": "dateTimeOffset",
                          "maxLength": -1
                        },
                        {
                          "name": "modifiedon",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "IsDelete",
                          "dataType": "boolean",
                          "maxLength": -1
                        }
                      ],
                      "partitions": []
                    },
                    {
                      "$type": "LocalEntity",
                      "name": "dimensionattributelevelvalue",
                      "description": "dimensionattributelevelvalue",
                      "annotations": [
                        {
                          "name": "Athena:PartitionGranularity",
                          "value": "Year"
                        },
                        {
                          "name": "Athena:InitialSyncState",
                          "value": "Completed"
                        },
                        {
                          "name": "Athena:InitialSyncDataCompletedTime",
                          "value": "1/1/2020 0:00:00 PM"
                        }
                      ],
                      "attributes": [
                        {
                          "name": "Id",
                          "dataType": "guid",
                          "maxLength": -1
                        },
                        {
                          "name": "SinkCreatedOn",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "SinkModifiedOn",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "sysdatastatecode",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "dimensionattributevalue",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "dimensionattributevaluegroup",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "displayvalue",
                          "dataType": "string",
                          "maxLength": 30,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 30
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "ordinal",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "backingrecorddataareaid",
                          "dataType": "string",
                          "maxLength": 4,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 4
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "modifieddatetime",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "modifiedby",
                          "dataType": "string",
                          "maxLength": 20,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 20
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "modifiedtransactionid",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "createddatetime",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "createdby",
                          "dataType": "string",
                          "maxLength": 20,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 20
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "createdtransactionid",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "dataareaid",
                          "dataType": "string",
                          "maxLength": 4,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 4
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "recversion",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "partition",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "sysrowversion",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "recid",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "tableid",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "versionnumber",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "createdon",
                          "dataType": "dateTimeOffset",
                          "maxLength": -1
                        },
                        {
                          "name": "modifiedon",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "IsDelete",
                          "dataType": "boolean",
                          "maxLength": -1
                        }
                      ],
                      "partitions": []
                    },
                    {
                      "$type": "LocalEntity",
                      "name": "dimensionattributelevel",
                      "description": "dimensionattributelevel",
                      "annotations": [
                        {
                          "name": "Athena:PartitionGranularity",
                          "value": "Year"
                        },
                        {
                          "name": "Athena:InitialSyncState",
                          "value": "Completed"
                        },
                        {
                          "name": "Athena:InitialSyncDataCompletedTime",
                          "value": "1/1/2020 0:00:00 PM"
                        }
                      ],
                      "attributes": [
                        {
                          "name": "Id",
                          "dataType": "guid",
                          "maxLength": -1
                        },
                        {
                          "name": "SinkCreatedOn",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "SinkModifiedOn",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "sysdatastatecode",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "dimensionattributevalue",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "dimensionattributevaluegroup",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "displayvalue",
                          "dataType": "string",
                          "maxLength": 30,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 30
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "ordinal",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "backingrecorddataareaid",
                          "dataType": "string",
                          "maxLength": 4,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 4
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "modifieddatetime",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "modifiedby",
                          "dataType": "string",
                          "maxLength": 20,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 20
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "modifiedtransactionid",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "createddatetime",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "createdby",
                          "dataType": "string",
                          "maxLength": 20,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 20
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "createdtransactionid",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "dataareaid",
                          "dataType": "string",
                          "maxLength": 4,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 4
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "recversion",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "partition",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "sysrowversion",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "recid",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "tableid",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "versionnumber",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "createdon",
                          "dataType": "dateTimeOffset",
                          "maxLength": -1
                        },
                        {
                          "name": "modifiedon",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "IsDelete",
                          "dataType": "boolean",
                          "maxLength": -1
                        }
                      ],
                      "partitions": []
                    }
                  ]
                }"""
MODEL_JSON_WITHOUT_DIMENSION_ATTRIBUTE_VALUE = """{
                  "name": "cdm",
                  "description": "cdm",
                  "version": "1.0",
                  "entities": [
                    {
                      "$type": "LocalEntity",
                      "name": "currency",
                      "description": "currency",
                      "annotations": [
                        {
                          "name": "Athena:PartitionGranularity",
                          "value": "Year"
                        },
                        {
                          "name": "Athena:InitialSyncState",
                          "value": "Completed"
                        },
                        {
                          "name": "Athena:InitialSyncDataCompletedTime",
                          "value": "1/1/2020 0:00:00 PM"
                        }
                      ],
                      "attributes": [
                        {
                          "name": "Id",
                          "dataType": "guid",
                          "maxLength": -1
                        },
                        {
                          "name": "SinkCreatedOn",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "SinkModifiedOn",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "iseuro",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "roundofftypeassetdep_jp",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "roundofftypeprice",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "roundofftypepurch",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "roundofftypesales",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "ltmroundofftypelineamount",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "sysdatastatecode",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "currencycode",
                          "dataType": "string",
                          "maxLength": 3,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 3
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "currencycodeiso",
                          "dataType": "string",
                          "maxLength": 3,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 3
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "roundingprecision",
                          "dataType": "decimal",
                          "maxLength": -1,
                          "cdm:traits": [
                            {
                              "traitReference": "is.dataFormat.numeric.shaped",
                              "arguments": [
                                {
                                  "name": "precision",
                                  "value": 38
                                },
                                {
                                  "name": "scale",
                                  "value": 6
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "roundoffassetdep_jp",
                          "dataType": "decimal",
                          "maxLength": -1,
                          "cdm:traits": [
                            {
                              "traitReference": "is.dataFormat.numeric.shaped",
                              "arguments": [
                                {
                                  "name": "precision",
                                  "value": 38
                                },
                                {
                                  "name": "scale",
                                  "value": 6
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "roundoffprice",
                          "dataType": "decimal",
                          "maxLength": -1,
                          "cdm:traits": [
                            {
                              "traitReference": "is.dataFormat.numeric.shaped",
                              "arguments": [
                                {
                                  "name": "precision",
                                  "value": 38
                                },
                                {
                                  "name": "scale",
                                  "value": 6
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "roundoffpurch",
                          "dataType": "decimal",
                          "maxLength": -1,
                          "cdm:traits": [
                            {
                              "traitReference": "is.dataFormat.numeric.shaped",
                              "arguments": [
                                {
                                  "name": "precision",
                                  "value": 38
                                },
                                {
                                  "name": "scale",
                                  "value": 6
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "roundoffsales",
                          "dataType": "decimal",
                          "maxLength": -1,
                          "cdm:traits": [
                            {
                              "traitReference": "is.dataFormat.numeric.shaped",
                              "arguments": [
                                {
                                  "name": "precision",
                                  "value": 38
                                },
                                {
                                  "name": "scale",
                                  "value": 6
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "symbol",
                          "dataType": "string",
                          "maxLength": 5,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 5
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "txt",
                          "dataType": "string",
                          "maxLength": 120,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 120
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "exchratemaxvariationpercent_mx",
                          "dataType": "decimal",
                          "maxLength": -1,
                          "cdm:traits": [
                            {
                              "traitReference": "is.dataFormat.numeric.shaped",
                              "arguments": [
                                {
                                  "name": "precision",
                                  "value": 38
                                },
                                {
                                  "name": "scale",
                                  "value": 6
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "decimalscount_mx",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "ltmroundofflineamount",
                          "dataType": "decimal",
                          "maxLength": -1,
                          "cdm:traits": [
                            {
                              "traitReference": "is.dataFormat.numeric.shaped",
                              "arguments": [
                                {
                                  "name": "precision",
                                  "value": 38
                                },
                                {
                                  "name": "scale",
                                  "value": 6
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "modifieddatetime",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "modifiedby",
                          "dataType": "string",
                          "maxLength": 20,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 20
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "modifiedtransactionid",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "createddatetime",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "createdby",
                          "dataType": "string",
                          "maxLength": 20,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 20
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "createdtransactionid",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "dataareaid",
                          "dataType": "string",
                          "maxLength": 4,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 4
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "recversion",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "partition",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "sysrowversion",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "recid",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "tableid",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "versionnumber",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "createdon",
                          "dataType": "dateTimeOffset",
                          "maxLength": -1
                        },
                        {
                          "name": "modifiedon",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "IsDelete",
                          "dataType": "boolean",
                          "maxLength": -1
                        }
                      ],
                      "partitions": []
                    },
                    {
                      "$type": "LocalEntity",
                      "name": "dimensionattributelevel",
                      "description": "dimensionattributelevel",
                      "annotations": [
                        {
                          "name": "Athena:PartitionGranularity",
                          "value": "Year"
                        },
                        {
                          "name": "Athena:InitialSyncState",
                          "value": "Completed"
                        },
                        {
                          "name": "Athena:InitialSyncDataCompletedTime",
                          "value": "1/1/2020 0:00:00 PM"
                        }
                      ],
                      "attributes": [
                        {
                          "name": "Id",
                          "dataType": "guid",
                          "maxLength": -1
                        },
                        {
                          "name": "SinkCreatedOn",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "SinkModifiedOn",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "sysdatastatecode",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "dimensionattributevalue",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "dimensionattributevaluegroup",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "displayvalue",
                          "dataType": "string",
                          "maxLength": 30,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 30
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "ordinal",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "backingrecorddataareaid",
                          "dataType": "string",
                          "maxLength": 4,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 4
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "modifieddatetime",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "modifiedby",
                          "dataType": "string",
                          "maxLength": 20,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 20
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "modifiedtransactionid",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "createddatetime",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "createdby",
                          "dataType": "string",
                          "maxLength": 20,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 20
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "createdtransactionid",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "dataareaid",
                          "dataType": "string",
                          "maxLength": 4,
                          "cdm:traits": [
                            {
                              "traitReference": "is.constrained",
                              "arguments": [
                                {
                                  "name": "maximumLength",
                                  "value": 4
                                }
                              ]
                            }
                          ]
                        },
                        {
                          "name": "recversion",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "partition",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "sysrowversion",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "recid",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "tableid",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "versionnumber",
                          "dataType": "int64",
                          "maxLength": -1
                        },
                        {
                          "name": "createdon",
                          "dataType": "dateTimeOffset",
                          "maxLength": -1
                        },
                        {
                          "name": "modifiedon",
                          "dataType": "dateTime",
                          "maxLength": -1
                        },
                        {
                          "name": "IsDelete",
                          "dataType": "boolean",
                          "maxLength": -1
                        }
                      ],
                      "partitions": []
                    }
                  ]
                }"""
MALFORMED_MODEL_JSON = """{"name": "cdm", "description": "cdm", "version": "1.0"}"""

AZURITE_CONNECTION_STRING='DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10001/devstoreaccount1'
CONTAINER = "cdm-e2e"
INCORRECT_SCHEMA_CONTAINER = "cdm-e2e-malformed-schema"
# Get the current date and time
now = datetime.utcnow()

# Subtract 6 hours
start_time = now - timedelta(hours=8)

# Generate formatted strings for each hour
FOLDERS = [(start_time + timedelta(hours=i)).strftime("%Y-%m-%dT%H.%M.%SZ") for i in range(8)]

blob_service_client = BlobServiceClient.from_connection_string(AZURITE_CONNECTION_STRING)
def upload_blob_file(blob_service_client: BlobServiceClient, container_name: str, blob_name: str, content: str):
    blob_service_client.get_container_client(container=container_name).upload_blob(name=blob_name, data=content.encode('utf-8'), overwrite=True)

def create_container():
   # Create a container for Azurite for the first run
   blob_service_client = BlobServiceClient.from_connection_string(AZURITE_CONNECTION_STRING)
   try:
      blob_service_client.create_container(CONTAINER)
   except Exception as e:
      print(e)

def create_faulty_container():
   blob_service_client = BlobServiceClient.from_connection_string(AZURITE_CONNECTION_STRING)
   try:
      blob_service_client.create_container(INCORRECT_SCHEMA_CONTAINER)
   except Exception as e:
      print(e)

def create_blobs():
    for folder in FOLDERS:
        upload_blob_file(blob_service_client, CONTAINER, f"{folder}/dimensionattributelevel/2020.csv", CONTENT)
        upload_blob_file(blob_service_client, CONTAINER, f"{folder}/dimensionattributelevel/2021.csv", CONTENT)
        upload_blob_file(blob_service_client, CONTAINER, f"{folder}/dimensionattributelevel/2022.csv", CONTENT)

        upload_blob_file(blob_service_client, INCORRECT_SCHEMA_CONTAINER, f"{folder}/dimensionattributelevel/2020.csv", CONTENT)
        upload_blob_file(blob_service_client, INCORRECT_SCHEMA_CONTAINER, f"{folder}/dimensionattributelevel/2021.csv", CONTENT)
        upload_blob_file(blob_service_client, INCORRECT_SCHEMA_CONTAINER, f"{folder}/dimensionattributelevel/2022.csv", CONTENT)

        if folder != FOLDERS[0]:
            upload_blob_file(blob_service_client, CONTAINER, f"{folder}/dimensionattributelevelvalue/2020.csv", CONTENT)
            upload_blob_file(blob_service_client, CONTAINER, f"{folder}/dimensionattributelevelvalue/4.csv", DELETE_FILE_CONTENT)
            upload_blob_file(blob_service_client, INCORRECT_SCHEMA_CONTAINER, f"{folder}/dimensionattributelevelvalue/2020.csv", CONTENT)
        if folder == FOLDERS[0]:
            upload_blob_file(blob_service_client, CONTAINER, f"{folder}/model.json", MODEL_JSON_WITHOUT_DIMENSION_ATTRIBUTE_VALUE)
        if folder == FOLDERS[1]:
            upload_blob_file(blob_service_client, INCORRECT_SCHEMA_CONTAINER, f"{folder}/model.json", MALFORMED_MODEL_JSON)
            upload_blob_file(blob_service_client, CONTAINER, f"{folder}/model.json", MODEL_JSON)
        else:
            upload_blob_file(blob_service_client, CONTAINER, f"{folder}/model.json", MODEL_JSON)
            upload_blob_file(blob_service_client, INCORRECT_SCHEMA_CONTAINER, f"{folder}/model.json", MODEL_JSON)

    upload_blob_file(blob_service_client, CONTAINER, "model.json", MODEL_JSON)
    upload_blob_file(blob_service_client, INCORRECT_SCHEMA_CONTAINER, "model.json", MODEL_JSON)


create_container()
create_faulty_container()
create_blobs()
upload_blob_file(blob_service_client, CONTAINER, "Changelog/changelog.info", FOLDERS[-1])
upload_blob_file(blob_service_client, INCORRECT_SCHEMA_CONTAINER, "Changelog/changelog.info", FOLDERS[-1])
