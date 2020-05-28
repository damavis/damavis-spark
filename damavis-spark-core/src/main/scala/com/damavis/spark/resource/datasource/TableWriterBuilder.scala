package com.damavis.spark.resource.datasource

import com.damavis.spark.database.{Database, Table}
import com.damavis.spark.resource.datasource.enums.Format.Format
import com.damavis.spark.resource.datasource.enums.OverwritePartitionBehavior._
import com.damavis.spark.resource.{ResourceWriter, WriterBuilder}
import org.apache.spark.sql.{SaveMode, SparkSession}

object TableWriterBuilder {
  def apply(table: Table)(implicit spark: SparkSession,
                          db: Database): TableWriterBuilder = {
    val params = TableWriterParameters()

    new TableWriterBuilder(table, db, params)
  }
}

class TableWriterBuilder(
    table: Table,
    db: Database,
    params: TableWriterParameters)(implicit spark: SparkSession)
    extends WriterBuilder {
  override def writer(): ResourceWriter =
    new TableResourceWriter(spark, table, db, params)

  def withFormat(format: Format): TableWriterBuilder = {
    val newParams = params.copy(storageFormat = format)

    new TableWriterBuilder(table, db, newParams)
  }

  def partitionedBy(columns: String*): TableWriterBuilder = {
    val newParams = params.copy(partitionedBy = Some(columns))

    new TableWriterBuilder(table, db, newParams)
  }

  def saveMode(saveMode: SaveMode): TableWriterBuilder = {
    val newParams = params.copy(saveMode = saveMode)

    new TableWriterBuilder(table, db, newParams)
  }

  def overwritePartitionBehavior(
      behavior: OverwritePartitionBehavior): TableWriterBuilder = {
    val newParams = params.copy(overwriteBehavior = behavior)

    new TableWriterBuilder(table, db, newParams)
  }

}
