package com.damavis.spark.resource.file.partitioning
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalUnit

import scala.collection.JavaConverters._

object DatePartitionFormatter {
  type ColumnFormatter = (String, DateTimeFormatter)

  def standard: DatePartitionFormatter = {
    val cols = ("year", DateTimeFormatter.ofPattern("yyyy")) ::
      ("month", DateTimeFormatter.ofPattern("MM")) ::
      ("day", DateTimeFormatter.ofPattern("dd")) ::
      Nil

    new DatePartitionFormatter(cols)
  }

  def apply(definitions: Seq[DatePartColumn]): DatePartitionFormatter = {
    val cols = definitions.map { columnDef =>
      try {
        (columnDef.columnName, DateTimeFormatter.ofPattern(columnDef.format))
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

class DatePartitionFormatter(
    columns: Seq[DatePartitionFormatter.ColumnFormatter]) {
  def dateToPath(date: LocalDateTime): String = {
    columns
      .map { p: (String, DateTimeFormatter) =>
        s"${p._1}=${p._2.format(date)}"
      }
      .mkString("/")
  }

  def columnNames: Seq[String] = columns.map { _._1 }

  def minimumTemporalUnit(): TemporalUnit = {
    implicit val ordering: Ordering[TemporalUnit] =
      (x: TemporalUnit, y: TemporalUnit) =>
        x.getDuration.compareTo(y.getDuration)

    columns
      .flatMap(_._2.getResolverFields.asScala)
      .map(_.getBaseUnit)
      .min
  }
}
