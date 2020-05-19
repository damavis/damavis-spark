package com.damavis.spark.resource.datasource

import com.damavis.spark.database.{Database, Table}
import com.damavis.spark.resource.{BasicResourceRW, RWBuilder, ResourceRW}
import org.apache.spark.sql.SparkSession

object TableRWBuilder {
  def apply(table: Table)(implicit spark: SparkSession,
                          db: Database): TableRWBuilder = {
    val writerParameters = TableWriterParameters()

    new TableRWBuilder(table, writerParameters)
  }

  def apply(table: Table, writeParameters: TableWriterParameters)(
      implicit spark: SparkSession,
      db: Database): TableRWBuilder =
    new TableRWBuilder(table, writeParameters)

}

class TableRWBuilder(table: Table, writeParams: TableWriterParameters)(
    implicit spark: SparkSession,
    db: Database)
    extends RWBuilder {
  override def build(): ResourceRW = {
    val reader = new TableReaderBuilder(table).reader()
    val writer = new TableWriterBuilder(table, writeParams).writer()

    new BasicResourceRW(reader, writer)
  }
}
