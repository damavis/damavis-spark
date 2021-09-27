package com.damavis.spark.resource.datasource.snowflake

import com.damavis.spark.resource.ResourceReader
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

case class SnowflakeJoinReader(reader: SnowflakeReader, joinData: DataFrame)(
    implicit spark: SparkSession)
  extends ResourceReader {

  assert(
    reader.table.isDefined,
    "SnowflakeJoinReader only accept table reads, not queries.")

  val stagingTable = s"join_tmp__${reader.table.get}"
  val targetTable = s"${reader.table.get}"

  private lazy val query =
    s"""
      |SELECT ${targetTable}.*
      |FROM ${targetTable}
      |INNER JOIN ${stagingTable}
      |ON ${joinExpression(joinData)}
      |""".stripMargin

  private def joinExpression(data: DataFrame): String = {
    data.columns
      .map(column => s"${targetTable}.${column} = ${stagingTable}.${column}")
      .mkString(" AND ")
  }

  override def read(): DataFrame = {
    SnowflakeWriter(
      reader.account,
      reader.user,
      reader.password,
      reader.warehouse,
      reader.database,
      reader.schema,
      stagingTable,
      SaveMode.Overwrite,
      reader.sfExtraOptions).write(joinData)
    reader.copy(table = None, query = Some(query)).read()
  }

}
