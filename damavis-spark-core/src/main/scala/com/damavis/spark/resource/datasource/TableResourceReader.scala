package com.damavis.spark.resource.datasource

import com.damavis.spark.database.Schema
import com.damavis.spark.resource.ResourceReader
import org.apache.spark.sql.{DataFrame, SparkSession}

class TableResourceReader(spark: SparkSession, schema: Schema, table: String)
    extends ResourceReader {
  override def read(): DataFrame =
    if (!schema.tableExists(table)) {
      throw new RuntimeException(s"Table $table not found in catalog")
    } else {
      spark.read.table(table)
    }
}
