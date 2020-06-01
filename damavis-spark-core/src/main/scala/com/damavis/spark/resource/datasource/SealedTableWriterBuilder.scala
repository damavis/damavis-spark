package com.damavis.spark.resource.datasource

import com.damavis.spark.database.exceptions.TableDefinitionException
import com.damavis.spark.database.{Database, Table}
import com.damavis.spark.resource.datasource.enums.Format.Format
import org.apache.spark.sql.SparkSession

class SealedTableWriterBuilder(
    table: Table,
    db: Database,
    params: TableWriterParameters)(implicit spark: SparkSession)
    extends BasicTableWriterBuilder(table, db, params) {
  override def withFormat(format: Format): BasicTableWriterBuilder = {
    if (table.format != format) {
      val msg =
        s"""Table ${table.name} already defined with a different format.
           |Previous format was: ${table.format}
           |Request format is: $format
           |""".stripMargin
      throw new TableDefinitionException(table.name, msg)
    }

    super.withFormat(format)
  }

  override def partitionedBy(columns: String*): BasicTableWriterBuilder = {
    if (!table.isPartitioned) {
      val msg =
        s"""Table ${table.name} is already defined with no partitioning"""
      throw new TableDefinitionException(table.name, msg)
    }

    val partitionColumnNames = table.partitions.map(_.name)
    if (partitionColumnNames != columns) {
      val msg =
        s"""Table ${table.name} is already defined with different partitioning columns
           |Partitioning on schema: ${partitionColumnNames.mkString(",")}
           |Requested partitioning: ${columns.mkString(",")}
           |""".stripMargin
      throw new TableDefinitionException(table.name, msg)
    }

    super.partitionedBy(columns: _*)
  }
}
