package com.damavis.spark.resource.file.partitioning

import java.time.LocalDateTime

class DateSplit(includeHour: Boolean = false) extends PartitionDateFormatter {
  def dateToPath(date: LocalDateTime): String = {
    val begin =
      s"year=${date.getYear}/month=${date.getMonthValue}/day=${date.getDayOfMonth}"

    if (includeHour) {
      s"$begin/h=${isoHour(date)}"
    } else {
      begin
    }
  }
}
