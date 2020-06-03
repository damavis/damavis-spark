package com.damavis.spark.resource.file.partitioning

import java.time.LocalDateTime

trait PartitionDateFormatter {
  def dateToPath(date: LocalDateTime): String

  def columnNames: Seq[String]

  protected def twoDigits(value: Int): String =
    if (value >= 10)
      s"$value"
    else
      s"0$value"
}
