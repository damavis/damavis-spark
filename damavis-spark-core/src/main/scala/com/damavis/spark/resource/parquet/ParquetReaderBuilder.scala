package com.damavis.spark.resource.parquet

import java.time.LocalDate

import com.damavis.spark.resource.{ReaderBuilder, ResourceReader}
import org.apache.spark.sql.SparkSession

object ParquetReaderBuilder {
  def apply(path: String)(
      implicit spark: SparkSession): ParquetReaderBuilder = {
    val params = ParquetReaderParameters(path, spark)
    new ParquetReaderBuilder(params)
  }

  def apply(params: ParquetReaderParameters): ParquetReaderBuilder =
    new ParquetReaderBuilder(params)
}

class ParquetReaderBuilder(params: ParquetReaderParameters)
    extends ReaderBuilder {

  override def reader(): ResourceReader = {
    if (params.datePartitioned)
      checkProperDates()

    new ParquetReader(params)
  }

  def betweenDates(from: LocalDate, to: LocalDate): ParquetReaderBuilder = {
    val newParams =
      params.copy(datePartitioned = true, from = Some(from), to = Some(to))

    new ParquetReaderBuilder(newParams)
  }

  private def checkProperDates(): Unit = {
    if (!(params.from.isDefined && params.to.isDefined)) {
      val errMsg =
        s"""Invalid parameters defined for reading spark object with path: ${params.path}.
           |Missing either "from" or "to" date (or both)
           |""".stripMargin
      throw new RuntimeException(errMsg)
    }

    val from = params.from.get
    val to = params.to.get

    if (from.isAfter(to)) {
      val errMsg =
        s"""Invalid parameters defined for reading spark object with path: ${params.path}.
           |"from" date is after "to" date.
           |Dates are: from=$from to=$to
           |""".stripMargin
      throw new RuntimeException(errMsg)
    }
  }
}
