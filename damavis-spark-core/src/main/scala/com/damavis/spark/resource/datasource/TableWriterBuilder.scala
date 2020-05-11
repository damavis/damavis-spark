package com.damavis.spark.resource.datasource

import com.damavis.spark.database.{Schema, Table, TableOptions}
import com.damavis.spark.resource.datasource.Format._
import com.damavis.spark.resource.datasource.OverwritePartitionBehavior._
import com.damavis.spark.resource.{ResourceWriter, WriterBuilder}
import org.apache.spark.sql.{SaveMode, SparkSession}

object TableWriterBuilder {
  def apply(table: Table)(implicit spark: SparkSession,
                          schema: Schema): TableWriterBuilder = {
    val params = TableWriterParameters()

    new TableWriterBuilder(table, params)
  }
}

class TableWriterBuilder(table: Table, params: TableWriterParameters)(
    implicit spark: SparkSession,
    schema: Schema)
    extends WriterBuilder {
  override def writer(): ResourceWriter =
    new TableResourceWriter(spark, table, params)

  def partitionedBy(columns: Seq[String]): TableWriterBuilder = {
    val newParams = params.copy(partitionedBy = Some(columns))

    new TableWriterBuilder(table, newParams)
  }

  def saveMode(saveMode: SaveMode): TableWriterBuilder = {
    val newParams = params.copy(saveMode = saveMode)

    new TableWriterBuilder(table, newParams)
  }

  def overwritePartitionBehavior(
      behavior: OverwritePartitionBehavior): TableWriterBuilder = {
    val newParams = params.copy(overwriteBehavior = behavior)

    new TableWriterBuilder(table, newParams)
  }

}
