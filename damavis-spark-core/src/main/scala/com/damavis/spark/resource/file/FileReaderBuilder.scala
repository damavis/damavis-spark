package com.damavis.spark.resource.file

import java.sql.Timestamp
import java.time.{LocalDate, LocalDateTime, LocalTime, ZoneId}

import com.damavis.spark.resource.Format.Format
import com.damavis.spark.resource.partitioning.DatePartitionFormatter
import com.damavis.spark.resource.{ReaderBuilder, ResourceReader}
import org.apache.spark.sql.SparkSession

object FileReaderBuilder {
  def apply(format: Format, path: String)(
      implicit spark: SparkSession): FileReaderBuilder = {
    val params = FileReaderParameters(format, path)
    new FileReaderBuilder(params)
  }

  def apply(params: FileReaderParameters)(
      implicit spark: SparkSession): FileReaderBuilder =
    new FileReaderBuilder(params)
}

class FileReaderBuilder(params: FileReaderParameters)(
    implicit spark: SparkSession)
    extends ReaderBuilder {

  override def reader(): ResourceReader = {
    if (params.datePartitioned)
      checkProperDates()

    new FileReader(params)
  }

  def partitioning(
      partitionFormatter: DatePartitionFormatter): FileReaderBuilder = {
    val newParams = params.copy(partitionFormatter = partitionFormatter)
    new FileReaderBuilder(newParams)
  }

  def options(options: Map[String, String]): FileReaderBuilder = {
    val newParams = params.copy(options = options)
    new FileReaderBuilder(newParams)
  }

  def betweenDates(from: LocalDate, to: LocalDate): FileReaderBuilder = {
    val time = LocalTime.of(0, 0, 0)
    betweenDates(LocalDateTime.of(from, time), LocalDateTime.of(to, time))
  }

  def betweenDates(from: LocalDateTime,
                   to: LocalDateTime): FileReaderBuilder = {
    val newParams =
      params.copy(from = Some(from), to = Some(to))

    new FileReaderBuilder(newParams)
  }

  def betweenDates(from: Timestamp, to: Timestamp): FileReaderBuilder = {
    betweenDates(
      LocalDateTime.ofInstant(from.toInstant, ZoneId.systemDefault()),
      LocalDateTime.ofInstant(to.toInstant, ZoneId.systemDefault()))
  }

  def partitionDateFormat(
      formatter: DatePartitionFormatter): FileReaderBuilder = {
    val newParams = params.copy(partitionFormatter = formatter)

    new FileReaderBuilder(newParams)
  }

  private def checkProperDates(): Unit = {
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
