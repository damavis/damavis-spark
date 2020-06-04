package com.damavis.spark.resource.file.partitioning
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object GenericDateFormatter {
  def apply(): GenericDateFormatter = {
    val cols = DatePartColumn("year", "yyyy") ::
      DatePartColumn("month", "MM") ::
      DatePartColumn("day", "dd") ::
      Nil

    new GenericDateFormatter(cols)
  }
}

class GenericDateFormatter(columns: Seq[DatePartColumn])
    extends PartitionDateFormatter {

  private val formatters = extractFormatters()

  private def extractFormatters(): Seq[DateTimeFormatter] =
    columns.map(columnDef => DateTimeFormatter.ofPattern(columnDef.format))

  override def dateToPath(date: LocalDateTime): String = {
    columns
      .zip(formatters)
      .map { p =>
        s"${p._1.columnName}=${p._2.format(date)}"
      }
      .mkString("/")
  }

  override def columnNames: Seq[String] = columns.map { _.columnName }
}
