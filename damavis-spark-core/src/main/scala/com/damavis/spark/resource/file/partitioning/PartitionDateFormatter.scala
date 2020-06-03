package com.damavis.spark.resource.file.partitioning

import java.time.LocalDateTime

trait PartitionDateFormatter {
  def dateToPath(date: LocalDateTime): String

  protected def isoHour(date: LocalDateTime): String =
    if (date.getHour >= 10)
      s"${date.getHour}"
    else
      s"0${date.getHour}"
}
