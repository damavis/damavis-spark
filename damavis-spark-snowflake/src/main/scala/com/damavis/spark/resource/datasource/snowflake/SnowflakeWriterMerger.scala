package com.damavis.spark.resource.datasource.snowflake

import com.damavis.spark.resource.ResourceWriter
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

case class SnowflakeWriterMerger(writer: SnowflakeWriter, columns: Seq[String])(
    implicit spark: SparkSession)
  extends ResourceWriter {

  val stagingTable = s"merge_tmp_delta__${writer.table}"
  val targetTable = s"${writer.table}"

  override def write(data: DataFrame): Unit = {
    if (targetExists()) {
      merge(data)
      writer.copy(mode = SaveMode.Append).write(data)
    } else {
      writer.copy(mode = SaveMode.Overwrite).write(data)
    }
  }

  private def merge(data: DataFrame): Unit = {
    writer
      .copy(table = stagingTable, mode = SaveMode.Overwrite)
      .write(data.select(columns.map(col): _*).distinct())

    SnowflakeMerger(
      writer.account,
      writer.user,
      writer.password,
      writer.warehouse,
      writer.database,
      writer.schema,
      stagingTable,
      targetTable,
      columns,
      writer.sfExtraOptions).merge()
  }

  private def targetExists(): Boolean = {
    val reader = SnowflakeReader(
      writer.account,
      writer.user,
      writer.password,
      writer.warehouse,
      writer.database,
      "INFORMATION_SCHEMA",
      query = Some(
        s"SELECT COUNT(1) = 1 FROM TABLES WHERE TABLE_NAME = '${targetTable}'"),
      sfExtraOptions = writer.sfExtraOptions
    )

    reader
      .read()
      .collect
      .map(_.getBoolean(0))
      .head
  }

}
