package com.damavis.spark.resource.datasource.snowflake

import com.damavis.spark.resource.ResourceReader
import org.apache.spark.sql.{DataFrame, SparkSession}

case class SnowflakeReader(
    account: String,
    user: String,
    password: String,
    warehouse: String,
    database: String,
    schema: String,
    table: Option[String] = None,
    query: Option[String] = None)(implicit spark: SparkSession)
    extends ResourceReader {

  val settings = (table, query) match {
    case (Some(tableName), None) => ("dbtable", tableName)
    case (None, Some(queryBody)) => ("query", queryBody)
    case (Some(_), Some(_)) =>
      throw new IllegalArgumentException(
        "SnowflakeReader cannot read table and query.")
  }

  val sfOptions = Map(
    "sfURL" -> s"${account}.snowflakecomputing.com",
    "sfUser" -> user,
    "sfPassword" -> password,
    "sfDatabase" -> database,
    "sfSchema" -> schema,
    "sfWarehouse" -> warehouse,
    "sfCompress" -> "on",
    "sfSSL" -> "on"
  ) + settings

  override def read(): DataFrame = {
    spark.read
      .format("net.snowflake.spark.snowflake")
      .options(sfOptions)
      .load()
  }
}
