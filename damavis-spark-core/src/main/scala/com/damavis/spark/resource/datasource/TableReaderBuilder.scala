package com.damavis.spark.resource.datasource

import com.damavis.spark.database.Schema
import com.damavis.spark.resource.{ReaderBuilder, ResourceReader}
import org.apache.spark.sql.SparkSession

class TableReaderBuilder(table: String)(implicit spark: SparkSession,
                                        schema: Schema)
    extends ReaderBuilder {
  override def reader(): ResourceReader =
    new TableResourceReader(spark, schema, table)
}
