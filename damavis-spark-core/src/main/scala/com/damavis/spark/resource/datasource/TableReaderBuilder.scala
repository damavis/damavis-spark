package com.damavis.spark.resource.datasource

import com.damavis.spark.database.exceptions.TableAccessException
import com.damavis.spark.database.{DummyTable, Table}
import com.damavis.spark.resource.{ReaderBuilder, ResourceReader}
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

object TableReaderBuilder {

  def apply(tryTable: Try[Table])(implicit spark: SparkSession): TableReaderBuilder = {
    tryTable match {
      case Success(table)     => apply(table)
      case Failure(exception) => throw exception
    }
  }

  def apply(table: Table)(implicit spark: SparkSession): TableReaderBuilder = {
    table match {
      case _: DummyTable =>
        val msg =
          s"""Table ${table.name} is not yet present in the catalog. No reads are possible"""
        throw new TableAccessException(msg)
      case _ => ()
    }

    new TableReaderBuilder(table, spark)
  }

}

class TableReaderBuilder protected (table: Table, spark: SparkSession)
  extends ReaderBuilder {

  override def reader(): ResourceReader =
    new TableResourceReader(spark, table)

}
