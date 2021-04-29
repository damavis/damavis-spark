package com.damavis.spark.database.exceptions

class TableDefinitionException(val table: String, msg: String) extends Exception(msg) {}
