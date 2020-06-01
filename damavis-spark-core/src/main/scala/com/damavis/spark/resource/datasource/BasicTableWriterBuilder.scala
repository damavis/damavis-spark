package com.damavis.spark.resource.datasource

import com.damavis.spark.database.{Database, DummyTable, RealTable, Table}
import com.damavis.spark.resource.datasource.enums.Format.Format
import com.damavis.spark.resource.datasource.enums.OverwritePartitionBehavior._
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
