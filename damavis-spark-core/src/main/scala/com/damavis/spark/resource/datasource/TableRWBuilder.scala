package com.damavis.spark.resource.datasource

import com.damavis.spark.database.Schema
import com.damavis.spark.resource.datasource.Format._
import com.damavis.spark.resource.{BasicResourceRW, RWBuilder, ResourceRW}
import org.apache.spark.sql.SparkSession

object TableRWBuilder {
  def apply(name: String, path: String, format: Format)(
      implicit spark: SparkSession,
      schema: Schema): TableRWBuilder = {
    val table = TableOptions(name, path, format)
    val writerParameters = TableWriterParameters(table)

    new TableRWBuilder(name, writerParameters)
  }

  def apply(table: String, writerParameters: TableWriterParameters)(
      implicit spark: SparkSession,
      schema: Schema): TableRWBuilder =
    new TableRWBuilder(table, writerParameters)

}

class TableRWBuilder(table: String, writeParams: TableWriterParameters)(
    implicit spark: SparkSession,
    schema: Schema)
    extends RWBuilder {
  override def build(): ResourceRW = {
    val reader = new TableReaderBuilder(table).reader()
    val writer = new TableWriterBuilder(writeParams).writer()

    new BasicResourceRW(reader, writer)
  }
}
