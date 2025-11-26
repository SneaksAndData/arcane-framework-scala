package com.sneaksanddata.arcane.framework
package tests.blobsource.json

object JsonSourceSchemas:
  val nestedArraySchema: String = """{
                                    |    "name": "BlobListingJsonSource",
                                    |    "namespace": "com.sneaksanddata.arcane.BlobListingJsonSource",
                                    |    "doc": "Avro Schema with nested fields for BlobListingJsonSource tests",
                                    |    "type": "record",
                                    |    "fields": [
                                    |        {
                                    |            "name": "col0",
                                    |            "type": [
                                    |                "null",
                                    |                "int"
                                    |            ],
                                    |            "default": null
                                    |        },
                                    |        {
                                    |            "name": "col1",
                                    |            "type": [
                                    |                "null",
                                    |                "string"
                                    |            ],
                                    |            "default": null
                                    |        },
                                    |        {
                                    |            "name": "col2",
                                    |            "type": [
                                    |                "null",
                                    |                "int"
                                    |            ],
                                    |            "default": null
                                    |        },
                                    |        {
                                    |            "name": "col3",
                                    |            "type": [
                                    |                "null",
                                    |                "string"
                                    |            ],
                                    |            "default": null
                                    |        },
                                    |        {
                                    |            "name": "col4",
                                    |            "type": [
                                    |                "null",
                                    |                "int"
                                    |            ],
                                    |            "default": null
                                    |        },
                                    |        {
                                    |            "name": "col5",
                                    |            "type": [
                                    |                "null",
                                    |                "string"
                                    |            ],
                                    |            "default": null
                                    |        },
                                    |        {
                                    |            "name": "col6",
                                    |            "type": [
                                    |                "null",
                                    |                "int"
                                    |            ],
                                    |            "default": null
                                    |        },
                                    |        {
                                    |            "name": "col7",
                                    |            "type": [
                                    |                "null",
                                    |                "string"
                                    |            ],
                                    |            "default": null
                                    |        },
                                    |        {
                                    |            "name": "col8",
                                    |            "type": [
                                    |                "null",
                                    |                "int"
                                    |            ],
                                    |            "default": null
                                    |        },
                                    |        {
                                    |            "name": "col9",
                                    |            "type": [
                                    |                "null",
                                    |                "string"
                                    |            ],
                                    |            "default": null
                                    |        },
                                    |        {
                                    |            "name": "nested_col_1",
                                    |            "type": [
                                    |                "null",
                                    |                "string"
                                    |            ],
                                    |            "default": null
                                    |        },
                                    |        {
                                    |            "name": "nested_col_2",
                                    |            "type": [
                                    |                "null",
                                    |                "int"
                                    |            ],
                                    |            "default": null
                                    |        }
                                    |    ]
                                    |}""".stripMargin

  val flatSchema: String =
    """{
      |    "name": "BlobListingJsonSource",
      |    "namespace": "com.sneaksanddata.arcane.BlobListingJsonSource",
      |    "doc": "Avro Schema for BlobListingJsonSource tests",
      |    "type": "record",
      |    "fields": [
      |        {
      |            "name": "col0",
      |            "type": [
      |                "null",
      |                "int"
      |            ],
      |            "default": null
      |        },
      |        {
      |            "name": "col1",
      |            "type": [
      |                "null",
      |                "string"
      |            ],
      |            "default": null
      |        },
      |        {
      |            "name": "col2",
      |            "type": [
      |                "null",
      |                "int"
      |            ],
      |            "default": null
      |        },
      |        {
      |            "name": "col3",
      |            "type": [
      |                "null",
      |                "string"
      |            ],
      |            "default": null
      |        },
      |        {
      |            "name": "col4",
      |            "type": [
      |                "null",
      |                "int"
      |            ],
      |            "default": null
      |        },
      |        {
      |            "name": "col5",
      |            "type": [
      |                "null",
      |                "string"
      |            ],
      |            "default": null
      |        },
      |        {
      |            "name": "col6",
      |            "type": [
      |                "null",
      |                "int"
      |            ],
      |            "default": null
      |        },
      |        {
      |            "name": "col7",
      |            "type": [
      |                "null",
      |                "string"
      |            ],
      |            "default": null
      |        },
      |        {
      |            "name": "col8",
      |            "type": [
      |                "null",
      |                "int"
      |            ],
      |            "default": null
      |        },
      |        {
      |            "name": "col9",
      |            "type": [
      |                "null",
      |                "string"
      |            ],
      |            "default": null
      |        }
      |    ]
      |}""".stripMargin
