package com.damavis.spark.resource.datasource

import com.damavis.spark.resource.datasource.Format._
import com.damavis.spark.resource.{BasicResourceRW, RWBuilder, ResourceRW}
import org.apache.spark.sql.SparkSession

object TableRWBuilder {
  def apply(format: Format, path: String, table: String)(
      implicit spark: SparkSession): TableRWBuilder = {
    val writerParameters = TableWriterParameters(format, path, table)

    new TableRWBuilder(table, writerParameters)
  }

  def apply(table: String, writerParameters: TableWriterParameters)(
      implicit spark: SparkSession): TableRWBuilder =
    new TableRWBuilder(table, writerParameters)

}

class TableRWBuilder(table: String, writeParams: TableWriterParameters)(
    implicit spark: SparkSession)
    extends RWBuilder {
  override def build(): ResourceRW = {
    val reader = new TableReaderBuilder(table).reader()
    val writer = new TableWriterBuilder(writeParams).writer()

    new BasicResourceRW(reader, writer)
  }
}
