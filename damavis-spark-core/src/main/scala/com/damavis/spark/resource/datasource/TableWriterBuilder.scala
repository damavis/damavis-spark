package com.damavis.spark.resource.datasource

import com.damavis.spark.database.Schema
import com.damavis.spark.resource.datasource.Format._
import com.damavis.spark.resource.datasource.OverwritePartitionBehavior._
import com.damavis.spark.resource.{ResourceWriter, WriterBuilder}
import org.apache.spark.sql.{SaveMode, SparkSession}

object TableWriterBuilder {
  def apply(name: String, path: String, format: Format)(
      implicit spark: SparkSession,
      schema: Schema): TableWriterBuilder = {
    val table = TableOptions(name, path, format)
    val params = TableWriterParameters(table)

    new TableWriterBuilder(params)
  }
}

class TableWriterBuilder(params: TableWriterParameters)(
    implicit spark: SparkSession,
    schema: Schema)
    extends WriterBuilder {
  override def writer(): ResourceWriter =
    new TableResourceWriter(spark, schema, params)

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
