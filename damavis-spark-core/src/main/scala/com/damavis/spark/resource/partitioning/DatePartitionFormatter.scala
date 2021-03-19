package com.damavis.spark.resource.partitioning
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoField, TemporalUnit}

import scala.collection.JavaConverters._

object DatePartitionFormatter {
  protected case class ColumnFormatter(column: String,
                                       pattern: String,
                                       formatter: DateTimeFormatter)

  def standard: DatePartitionFormatter = {
    val cols = DatePartColumn("year", "yyyy") ::
      DatePartColumn("month", "MM") ::
      DatePartColumn("day", "dd") ::
      Nil

    apply(cols)
  }

  def standardHourly: DatePartitionFormatter = {
    val cols = DatePartColumn("year", "yyyy") ::
      DatePartColumn("month", "MM") ::
      DatePartColumn("day", "dd") ::
      DatePartColumn("hour", "H") ::
      Nil

    apply(cols)
  }

  def daily: DatePartitionFormatter = {
    val cols = DatePartColumn("dt", "yyyy-MM-dd") :: Nil
    apply(cols)
  }

  def dailyHourly: DatePartitionFormatter = {
    val cols = DatePartColumn("dt", "yyyy-MM-dd") ::
      DatePartColumn("hour", "H") ::
      Nil
    apply(cols)
  }

  def apply(definitions: Seq[DatePartColumn]): DatePartitionFormatter = {
    if (definitions.isEmpty)
      throw new IllegalArgumentException(
        "Column definitions for a DatePartitionFormatter cannot be empty")

    val cols = definitions.map { columnDef =>
      try {
        val formatter = DateTimeFormatter.ofPattern(columnDef.format)
        ColumnFormatter(columnDef.columnName, columnDef.format, formatter)
      } catch {
        case ex: IllegalArgumentException =>
          val msg =
            s"""Invalid DateTimeFormatter "${columnDef.format}" specified for column ${columnDef.columnName}
               | """.stripMargin
          throw new IllegalArgumentException(msg, ex)
      }
    }
    new DatePartitionFormatter(cols)
  }
}

class DatePartitionFormatter protected (
    columns: Seq[DatePartitionFormatter.ColumnFormatter]) {
  def dateToPath(date: LocalDateTime): String = {
    columns
      .map { part =>
        s"${part.column}=${part.formatter.format(date)}"
      }
      .mkString("/")
  }

  def columnNames: Seq[String] = columns.map { _.column }

  def minimumTemporalUnit(): TemporalUnit = {
    //FIXME if custom literals are defined, and they contain either "H" or "h", this function will assume that the
    //      format contains hours. Those literals should be removed before checking for hours
    val containsHours = columns.exists { p =>
      p.pattern.contains("H") || p.pattern.contains("h")
    }

    val temporalField =
      if (containsHours)
        ChronoField.HOUR_OF_DAY
      else
        ChronoField.DAY_OF_MONTH

    temporalField.getBaseUnit
  }

  def isYearlyPartitioned: Boolean =
    columns.nonEmpty && columns.head.pattern == "yyyy"
}
