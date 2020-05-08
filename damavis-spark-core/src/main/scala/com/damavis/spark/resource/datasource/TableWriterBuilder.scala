package com.damavis.spark.resource.datasource

import com.damavis.spark.database.Schema
import com.damavis.spark.resource.datasource.Format._
import com.damavis.spark.resource.datasource.OverwritePartitionBehavior._
import com.damavis.spark.resource.{ResourceWriter, WriterBuilder}
import org.apache.spark.sql.{SaveMode, SparkSession}

object TableWriterBuilder {
  def apply(format: Format, path: String, table: String)(
      implicit spark: SparkSession): TableWriterBuilder = {
    val params = TableWriterParameters(format, path, table)

    new TableWriterBuilder(params)
  }
}

class TableWriterBuilder(params: TableWriterParameters)(
    implicit spark: SparkSession)
    extends WriterBuilder {
  override def writer(): ResourceWriter =
    new TableResourceWriter(spark, new Schema(spark), params)

  def partitionedBy(columns: Seq[String]): TableWriterBuilder = {
    val newParams = params.copy(partitionedBy = Some(columns))

    new TableWriterBuilder(newParams)
  }

  def saveMode(saveMode: SaveMode): TableWriterBuilder = {
    val newParams = params.copy(saveMode = saveMode)

    new TableWriterBuilder(newParams)
  }

  def overwritePartitionBehavior(
      behavior: OverwritePartitionBehavior): TableWriterBuilder = {
    val newParams = params.copy(overwriteBehavior = behavior)

    new TableWriterBuilder(newParams)
  }

}
