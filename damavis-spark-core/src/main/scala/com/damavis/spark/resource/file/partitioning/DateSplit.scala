package com.damavis.spark.resource.file.partitioning

import java.time.LocalDateTime

class DateSplit(includeHour: Boolean = false) extends PartitionDateFormatter {
  override def dateToPath(date: LocalDateTime): String = {
    val begin =
      s"year=${date.getYear}/month=${twoDigits(date.getMonthValue)}/day=${twoDigits(date.getDayOfMonth)}"

    if (includeHour)
      s"$begin/h=${twoDigits(date.getHour)}"
    else
      begin
  }

  override def columnNames: Seq[String] = {
    val dateCols = "year" :: "month" :: "day" :: Nil
    val hourCol =
      if (includeHour)
        "h" :: Nil
      else Nil

    dateCols ++ hourCol
  }
}
