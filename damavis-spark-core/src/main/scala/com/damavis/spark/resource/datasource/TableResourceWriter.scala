package com.damavis.spark.resource.datasource

import com.damavis.spark.database.Table
import com.damavis.spark.resource.ResourceWriter
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import enums.OverwritePartitionBehavior

class TableResourceWriter(spark: SparkSession,
                          table: Table,
                          params: TableWriterParameters)
    extends ResourceWriter {
  override def write(data: DataFrame): Unit = {
    if (!table.schema.tableExists(table.name))
      throw new RuntimeException(s"Table ${table.name} not found in schema")

    val previousOverwriteConf =
      spark.conf.get("spark.sql.sources.partitionOverwriteMode")

    if (params.saveMode == SaveMode.Overwrite && params.partitionedBy.isDefined) {
      val overwriteMode = params.overwriteBehavior match {
        case OverwritePartitionBehavior.OVERWRITE_ALL      => "static"
        case OverwritePartitionBehavior.OVERWRITE_MATCHING => "dynamic"
      }

      spark.conf.set("spark.sql.sources.partitionOverwriteMode", overwriteMode)
    }

    val writer = data.write

    val partitionWriter = params.partitionedBy match {
      case Some(columns) => writer.partitionBy(columns: _*)
      case None          => writer
    }

    try {
      partitionWriter
        .format(s"${table.options.format}")
        .option("path", table.options.path)
        .mode(params.saveMode)
        .saveAsTable(table.name)
    } catch {
      case e: Throwable => throw e
    } finally {
      spark.conf
        .set("spark.sql.sources.partitionOverwriteMode", previousOverwriteConf)
    }

  }
}
