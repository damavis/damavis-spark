package com.damavis.spark.resource.datasource.snowflake

import net.snowflake.spark.snowflake.Utils
import org.apache.spark.sql.SparkSession

case class SnowflakeMerger(
    account: String,
    user: String,
    password: String,
    warehouse: String,
    database: String,
    schema: String,
    sourceTable: String,
    targetTable: String,
    pkColumns: Seq[String])(implicit spark: SparkSession) {

  val sfOptions = Map(
    "sfURL" -> s"${account}.snowflakecomputing.com",
    "sfUser" -> user,
    "sfPassword" -> password,
    "sfDatabase" -> database,
    "sfSchema" -> schema,
    "sfWarehouse" -> warehouse,
    "sfCompress" -> "on",
    "sfSSL" -> "on"
  )

  private def mergeExpression(pkColumns: Seq[String]): String = {
    pkColumns
      .zip(pkColumns)
      .map(tuple => s"${targetTable}.${tuple._1} = ${sourceTable}.${tuple._2}")
      .mkString(" AND ")
  }

  def merge(): Unit = {
    val deleteQuery =
      s"""
        |MERGE INTO ${targetTable} USING ${sourceTable}
        |ON ${mergeExpression(pkColumns)}
        |WHEN MATCHED THEN DELETE
        |""".stripMargin

    Utils.runQuery(sfOptions, deleteQuery)
  }

}
