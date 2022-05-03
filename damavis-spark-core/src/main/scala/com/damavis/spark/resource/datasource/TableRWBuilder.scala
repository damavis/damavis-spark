package com.damavis.spark.resource.datasource

import com.damavis.spark.database.{Database, Table}
import com.damavis.spark.resource.{BasicResourceRW, RWBuilder, ResourceRW}
import org.apache.spark.sql.SparkSession

class TableRWBuilder(table: Table)(implicit spark: SparkSession, db: Database)
  extends RWBuilder {

  override def build(): ResourceRW = {
    val reader = TableReaderBuilder(table).reader()
    val writer = TableWriterBuilder(table).writer()

    new BasicResourceRW(reader, writer)
  }

}
