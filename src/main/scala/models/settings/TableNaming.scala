package com.sneaksanddata.arcane.framework
package models.settings

type TableName = String

object TableNaming:
  extension (tableName: TableName)
    def parts: (warehouse: String, namespace: String, name: String) =
      tableName.split ('.').toList match
        case warehouse :: namespace :: name :: _ => (warehouse = warehouse, namespace = namespace, name = name)
        case _ => throw new RuntimeException (s"Invalid table name format for $tableName. Must be {warehouse}.{namespace}.{name}")
