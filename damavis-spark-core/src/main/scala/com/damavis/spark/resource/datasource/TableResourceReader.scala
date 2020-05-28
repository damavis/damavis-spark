package com.damavis.spark.resource.datasource

import com.damavis.spark.database.exceptions.TableAccessException
import com.damavis.spark.database.{DummyTable, Table}
import com.damavis.spark.resource.ResourceReader
import org.apache.spark.sql.{DataFrame, SparkSession}

class TableResourceReader(spark: SparkSession, table: Table)
    extends ResourceReader {
  override def read(): DataFrame = {
    table match {
      case _: DummyTable =>
        val msg =
          s"""Table ${table.name} has not been written yet. No reads are possible"""
        throw new TableAccessException(msg)
      case _ => ()
    }

    spark.read.table(table.name)
  }

}
