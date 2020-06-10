package com.damavis.spark.resource.datasource

import com.damavis.spark.database.exceptions.TableDefinitionException
import com.damavis.spark.database.{Database, DummyTable, RealTable, Table}
import com.damavis.spark.resource.Format.Format
import com.damavis.spark.resource.datasource.OverwritePartitionBehavior._
import com.damavis.spark.resource.partitioning.DatePartitionFormatter
import com.damavis.spark.resource.{ResourceWriter, WriterBuilder}
import org.apache.spark.sql.{SaveMode, SparkSession}

object TableWriterBuilder {
  def apply(table: Table)(implicit spark: SparkSession,
                          db: Database): BasicTableWriterBuilder = {
    val params = TableWriterParameters()
    table match {
      case _: DummyTable => new BasicTableWriterBuilder(table, db, params)
      case _: RealTable  => new SealedTableWriterBuilder(table, db, params)
    }
  }
}

class BasicTableWriterBuilder(
    table: Table,
    db: Database,
    params: TableWriterParameters)(implicit spark: SparkSession)
    extends WriterBuilder {

  private var myParams: TableWriterParameters = params

  override def writer(): ResourceWriter =
    new TableResourceWriter(spark, table, db, myParams)

  def withFormat(format: Format): BasicTableWriterBuilder = {
    myParams = myParams.copy(storageFormat = format)

    this
  }

  def partitionedBy(columns: String*): BasicTableWriterBuilder = {
    myParams = myParams.copy(partitionedBy = Some(columns))

    this
  }

  def datePartitioned(): BasicTableWriterBuilder = {
    val formatter = DatePartitionFormatter.standard

    datePartitioned(formatter)
  }

  def datePartitioned(
      formatter: DatePartitionFormatter): BasicTableWriterBuilder = {
    partitionedBy(formatter.columnNames: _*)
  }

  def saveMode(saveMode: SaveMode): BasicTableWriterBuilder = {
    myParams = myParams.copy(saveMode = saveMode)

    this
  }

  def overwritePartitionBehavior(
      behavior: OverwritePartitionBehavior): BasicTableWriterBuilder = {
    myParams = myParams.copy(overwriteBehavior = behavior)

    this
  }

}

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

    val partitionColumnNames = table.columns.filter(_.partitioned).map(_.name)
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
