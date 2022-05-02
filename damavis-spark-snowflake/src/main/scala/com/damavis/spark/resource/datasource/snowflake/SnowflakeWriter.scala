package com.damavis.spark.resource.datasource.snowflake

import com.damavis.spark.resource.ResourceWriter
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

case class SnowflakeWriter(account: String,
                           user: String,
                           password: String,
                           warehouse: String,
                           database: String,
                           schema: String,
                           table: String,
                           mode: SaveMode = SaveMode.Ignore,
                           sfExtraOptions: Map[String, String] = Map(),
                           preScript: Option[String] = None)(implicit spark: SparkSession)
    extends ResourceWriter {

  val sfOptions = Map(
    "sfURL" -> s"${account}.snowflakecomputing.com",
    "sfUser" -> user,
    "sfPassword" -> password,
    "sfDatabase" -> database,
    "sfSchema" -> schema,
    "sfWarehouse" -> warehouse,
    "dbtable" -> table,
    "sfCompress" -> "on",
    "sfSSL" -> "on")

  override def write(data: DataFrame): Unit = {
    data.write
      .format("net.snowflake.spark.snowflake")
      .options(sfOptions ++ sfExtraOptions)
      .mode(mode)
      .save()
  }

}
