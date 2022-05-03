package com.damavis.spark.resource.datasource.snowflake

import com.damavis.spark.resource.ResourceWriter
import net.snowflake.spark.snowflake.Utils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Merge writer data to target table by specified columns
  *
  * @param writer Snowflake writer
  * @param columns columns used to merge on
  * @param stagingSchema staging database schema
  * @param deleteStagingTable Delete staging table if true
  * @param spark SparkSession object
  */
case class SnowflakeWriterMerger(writer: SnowflakeWriter,
                                 columns: Seq[String],
                                 stagingSchema: Option[String] = None,
                                 deleteStagingTable: Boolean = false)(
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
      stagingSchema.getOrElse(writer.schema),
      stagingTable,
      targetTable,
      columns,
      writer.sfExtraOptions).merge()

    if (deleteStagingTable) dropStagingTable()
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

  private def dropStagingTable(): Unit = {

    val deleteSourceTableQuery =
      s"DROP TABLE IF EXISTS $stagingTable RESTRICT"

    Utils.runQuery(writer.sfOptions, deleteSourceTableQuery)
  }

}
