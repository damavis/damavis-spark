package com.damavis.spark.resource.datasource

import com.damavis.spark.database.Table
import com.damavis.spark.resource.ResourceReader
import org.apache.spark.sql.{DataFrame, SparkSession}

class TableResourceReader(spark: SparkSession, table: Table)
    extends ResourceReader {
  override def read(): DataFrame = spark.read.table(table.name)

}
