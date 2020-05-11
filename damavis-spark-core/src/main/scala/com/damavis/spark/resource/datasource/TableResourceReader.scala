package com.damavis.spark.resource.datasource

import com.damavis.spark.database.Table
import com.damavis.spark.resource.ResourceReader
import org.apache.spark.sql.{DataFrame, SparkSession}

class TableResourceReader(spark: SparkSession, table: Table)
    extends ResourceReader {
  override def read(): DataFrame =
    if (!table.schema.tableExists(table.name)) {
      throw new RuntimeException(s"Table $table not found in catalog")
    } else {
      spark.read.table(table.name)
    }
}
