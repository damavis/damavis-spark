package com.damavis.spark.resource

import java.time.LocalDate
import com.damavis.spark.fs.FileSystem

private[resource] object DatePaths {
  def generate(path: String, startDate: LocalDate, endDate: LocalDate)(
      implicit fs: FileSystem): List[String] = {
    datesGen(startDate, endDate).par
      .map(date => dateToPath(path, date))
      .filter(fs.pathExists)
      .toList
  }

  private def datesGen(date1: LocalDate, date2: LocalDate): List[LocalDate] = {
    val (from, to) = {
      if (date2.isAfter(date1)) (date1, date2)
      else if (date1.isAfter(date2)) (date2, date1)
      else (date1, date1)
    }

    def datesGen(acc: List[LocalDate],
                 pointer: LocalDate,
                 end: LocalDate): List[LocalDate] = {
      if (pointer.isAfter(end)) acc
      else datesGen(acc :+ pointer, pointer.plusDays(1), end)
    }
    datesGen(List(), from, to)
  }

  private def dateToPath(basePath: String, date: LocalDate): String = {
    s"${basePath}/year=${date.getYear}/month=${date.getMonthValue}/day=${date.getDayOfMonth}"
  }
}
