package com.damavis.spark.resource.datasource

import com.damavis.spark.database.exceptions.TableAccessException
import com.damavis.spark.database.{Database, Table}
import com.damavis.spark.resource.{Format, ResourceWriter}
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.{col, lit}
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

  private def mergeExpression(partitions: Seq[String]): String = {
    partitions
      .zip(partitions)
      .map(tuple => s"target.${tuple._1} = update.${tuple._2}")
      .mkString(" AND ")
  }

  override def write(data: DataFrame): Unit = {
    updateCatalogBeforeWrite(data)

    if (params.storageFormat == Format.Delta) {
      if (params.partitionedBy.isDefined) {
        params.overwriteBehavior match {
          case OverwritePartitionBehavior.OVERWRITE_ALL =>
            data.write
              .mode(params.saveMode)
              .insertInto(actualTable.name)
          case OverwritePartitionBehavior.OVERWRITE_MATCHING =>
            val delta = DeltaTable.forName(s"${db.name}.${actualTable.name}")

            // Delete the whole partition
            val toDelete =
              data.select(params.partitionedBy.get.map(col): _*).distinct()
            val columns = toDelete.columns
            val toDeleteExpr = toDelete
              .collect()
              .map(_.toSeq)
              .map(v => columns.zip(v))
              .map(l => l.map(e => col(e._1) === lit(e._2)).reduce(_ && _))
              .reduceOption(_ || _)

            toDeleteExpr match {
              case Some(matching) =>
                delta
                  .as("target")
                  .delete(matching)
              case None => // Do nothing
            }

            // And insert it again
            data.write.mode(SaveMode.Append).insertInto(actualTable.name)
        }
      } else {
        params.overwriteBehavior match {
          case OverwritePartitionBehavior.OVERWRITE_ALL =>
            data.write
              .mode(params.saveMode)
              .insertInto(actualTable.name)
          case OverwritePartitionBehavior.OVERWRITE_MATCHING =>
            if (params.pk.isEmpty) {
              throw new TableAccessException(
                "Cannot overwrite dynamically delta tables without defining partitioning or a PrimaryKey")
            }

            val delta = DeltaTable.forName(s"${db.name}.${actualTable.name}")

            // Delete matching.
            val toDelete = data.select(params.pk.get.map(col): _*).distinct()
            delta
              .as("target")
              .merge(toDelete.as("update"), mergeExpression(params.pk.get))
              .whenMatched
              .delete()
              .execute()

            // And insert
            data.write.mode(SaveMode.Append).insertInto(actualTable.name)
        }
      }
    } else {

      val previousOverwriteConf =
        spark.conf.get("spark.sql.sources.partitionOverwriteMode")

      if (params.saveMode == SaveMode.Overwrite && params.partitionedBy.isDefined) {
        val overwriteMode = params.overwriteBehavior match {
          case OverwritePartitionBehavior.OVERWRITE_ALL      => "static"
          case OverwritePartitionBehavior.OVERWRITE_MATCHING => "dynamic"
        }

        spark.conf
          .set("spark.sql.sources.partitionOverwriteMode", overwriteMode)
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
          .set("spark.sql.sources.partitionOverwriteMode",
               previousOverwriteConf)
      }
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
