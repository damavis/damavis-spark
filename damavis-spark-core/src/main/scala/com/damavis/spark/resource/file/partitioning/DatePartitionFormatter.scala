package com.damavis.spark.resource.file.partitioning
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object DatePartitionFormatter {
  def standard: DatePartitionFormatter = {
    val cols = DatePartColumn("year", "yyyy") ::
      DatePartColumn("month", "MM") ::
      DatePartColumn("day", "dd") ::
      Nil

    new DatePartitionFormatter(cols)
  }
}

class DatePartitionFormatter(columns: Seq[DatePartColumn]) {

  private val formatters = extractFormatters()

  def dateToPath(date: LocalDateTime): String = {
    columns
      .zip(formatters)
      .map { p =>
        s"${p._1.columnName}=${p._2.format(date)}"
      }
      .mkString("/")
  }

  def columnNames: Seq[String] = columns.map { _.columnName }

  private def extractFormatters(): Seq[DateTimeFormatter] =
    columns.map(columnDef => DateTimeFormatter.ofPattern(columnDef.format))

}
