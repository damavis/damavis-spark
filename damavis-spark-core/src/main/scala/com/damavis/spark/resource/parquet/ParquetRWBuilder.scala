package com.damavis.spark.resource.parquet

import java.time.LocalDate

import com.damavis.spark.resource.{BasicResourceRW, RWBuilder, ResourceRW}
import org.apache.spark.sql.SparkSession

object ParquetRWBuilder {
  def apply(path: String)(implicit spark: SparkSession): ParquetRWBuilder = {
    val readParams = ParquetReaderParameters(path, spark)
    val writeParams = ParquetWriterParameters(path)

    new ParquetRWBuilder(readParams, writeParams)
  }
}

class ParquetRWBuilder(readParams: ParquetReaderParameters,
                       writeParams: ParquetWriterParameters)
    extends RWBuilder {
  override def build(): ResourceRW = {
    val reader = ParquetReaderBuilder(readParams).reader()
    val writer = ParquetWriterBuilder(writeParams).writer()

    new BasicResourceRW(reader, writer)
  }

  def betweenDates(from: LocalDate, to: LocalDate): ParquetRWBuilder = {
    val newReadParams =
      readParams.copy(datePartitioned = true, from = Some(from), to = Some(to))
    val newWriteParams =
      writeParams.copy(columnNames = "year" :: "month" :: "day" :: Nil)

    new ParquetRWBuilder(newReadParams, newWriteParams)
  }

  def writeMode(mode: String): ParquetRWBuilder =
    new ParquetRWBuilder(readParams, writeParams.copy(mode = mode))
}
