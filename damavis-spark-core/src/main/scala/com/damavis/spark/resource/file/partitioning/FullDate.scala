package com.damavis.spark.resource.file.partitioning

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class FullDate(includeHour: Boolean = false) extends PartitionDateFormatter {
  def dateToPath(date: LocalDateTime): String = {
    val begin = s"dt=${date.format(DateTimeFormatter.ISO_LOCAL_DATE)}"

    if (includeHour) {
      s"$begin/h=${isoHour(date)}"
    } else
      begin
  }
}
