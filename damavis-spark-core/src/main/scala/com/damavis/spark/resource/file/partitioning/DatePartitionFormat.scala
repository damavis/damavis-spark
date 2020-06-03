package com.damavis.spark.resource.file.partitioning

import java.time.LocalDateTime

import com.damavis.spark.fs.FileSystem

abstract class DatePartitionFormat(fs: FileSystem) {
  def dateToPath(date: LocalDateTime): String

  def generatePaths(from: LocalDateTime, to: LocalDateTime): Seq[String] = {
    datesGen(from, to)
      .map(dateToPath)
      .par
      .filter(fs.pathExists)
      .seq
  }

  private def datesGen(date1: LocalDateTime,
                       date2: LocalDateTime): List[LocalDateTime] = {
    val (from, to) = {
      if (date2.isAfter(date1)) (date1, date2)
      else if (date1.isAfter(date2)) (date2, date1)
      else (date1, date1)
    }

    def datesGen(acc: List[LocalDateTime],
                 pointer: LocalDateTime,
                 end: LocalDateTime): List[LocalDateTime] = {
      if (pointer.isAfter(end)) acc
      else datesGen(acc :+ pointer, pointer.plusDays(1), end)
    }
    datesGen(List(), from, to)
  }
}