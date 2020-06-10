package com.damavis.spark.resource.datasource

import com.damavis.spark.database.exceptions.TableAccessException
import com.damavis.spark.database.{Database, Table}
import com.damavis.spark.resource.ResourceWriter
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class TableResourceWriter(spark: SparkSession,
                          table: Table,
                          db: Database,
                          params: TableWriterParameters)
    extends ResourceWriter {

  private var actualTable: Table = table

  private def updateCatalogBeforeWrite(data: DataFrame): Unit = {
    val schema = data.schema
    val format = params.storageFormat
    val partitionedBy = params.partitionedBy.getOrElse(Nil)

    actualTable =
      db.addTableIfNotExists(actualTable, schema, format, partitionedBy)
  }

  override def write(data: DataFrame): Unit = {
    updateCatalogBeforeWrite(data)

    val previousOverwriteConf =
      spark.conf.get("spark.sql.sources.partitionOverwriteMode")

    if (params.saveMode == SaveMode.Overwrite && params.partitionedBy.isDefined) {
      val overwriteMode = params.overwriteBehavior match {
        case OverwritePartitionBehavior.OVERWRITE_ALL      => "static"
        case OverwritePartitionBehavior.OVERWRITE_MATCHING => "dynamic"
      }

      spark.conf.set("spark.sql.sources.partitionOverwriteMode", overwriteMode)
    }

    try {
      checkDataFrameColumns(data)

      data.write
        .mode(params.saveMode)
        .insertInto(actualTable.name)
    } catch {
      case e: Throwable => throw e
    } finally {
      spark.conf
        .set("spark.sql.sources.partitionOverwriteMode", previousOverwriteConf)
    }

  }

  private def checkDataFrameColumns(data: DataFrame): Unit = {
    // This step is mandatory since we use insertInto(), which does not respect the table's schema
    // Instead, it uses the columns' positions. So we have to check that the columns are in the proper order
    // defined by the schema

    val columnsDoNotMatch = actualTable.columns
      .zip(data.schema)
      .exists(p => p._1.name != p._2.name)

    if (columnsDoNotMatch) {
      val msg =
        s"""DataFrame to be written on ${actualTable.name} does not have columns in required order
           |Column order is: ${actualTable.columns}
           |""".stripMargin
      throw new TableAccessException(msg)
    }
  }

}
