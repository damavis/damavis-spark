package com.damavis.spark.resource.datasource

import com.damavis.spark.database.Schema
import com.damavis.spark.resource.ResourceWriter
import com.damavis.spark.resource.datasource.TableWriterParameters.OverwritePartitionBehavior
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class TableResourceWriter(spark: SparkSession,
                          schema: Schema,
                          params: TableWriterParameters)
    extends ResourceWriter {
  override def write(data: DataFrame): Unit = {
    if (!schema.tableExists(params.table))
      throw new RuntimeException(s"Table ${params.table} not found in schema")

    val previousOverwriteConf =
      spark.conf.get("spark.sql.sources.partitionOverwriteMode")

    if (params.saveMode == SaveMode.Overwrite && params.partitionedBy.isDefined) {
      val overwriteMode =
        if (params.overwriteBehavior == OverwritePartitionBehavior.OVERWRITE_ALL)
          "static"
        else
          "dynamic"

      spark.conf.set("spark.sql.sources.partitionOverwriteMode", overwriteMode)
    }

    val writer = data.write

    val partitionWriter = params.partitionedBy match {
      case Some(columns) => writer.partitionBy(columns: _*)
      case None          => writer
    }

    try {
      partitionWriter
        .format(params.format)
        .option("path", params.path)
        .mode(params.saveMode)
        .saveAsTable(params.table)
    } catch {
      case e: Throwable => throw e
    } finally {
      spark.conf
        .set("spark.sql.sources.partitionOverwriteMode", previousOverwriteConf)
    }

  }
}
