package com.damavis.spark.resource.file

import java.time.LocalDate

import com.damavis.spark.resource.Format.Format
import com.damavis.spark.resource.{ReaderBuilder, ResourceReader}
import org.apache.spark.sql.SparkSession

object FileReaderBuilder {
  def apply(format: Format, path: String)(
      implicit spark: SparkSession): FileReaderBuilder = {
    val params = FileReaderParameters(format, path, spark)
    new FileReaderBuilder(params)
  }

  def apply(params: FileReaderParameters): FileReaderBuilder =
    new FileReaderBuilder(params)
}

class FileReaderBuilder(params: FileReaderParameters) extends ReaderBuilder {

  override def reader(): ResourceReader = {
    if (params.datePartitioned)
      checkProperDates()

    new FileReader(params)
  }

  def betweenDates(from: LocalDate, to: LocalDate): FileReaderBuilder = {
    val newParams =
      params.copy(datePartitioned = true, from = Some(from), to = Some(to))

    new FileReaderBuilder(newParams)
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
