package com.damavis.spark.resource.partitioning

import java.time.LocalDateTime
import com.damavis.spark.fs.{FileSystem, HadoopFS}
import org.apache.spark.sql.SparkSession

object DatePartitions {
  def apply(root: String, pathGenerator: DatePartitionFormatter)(
      implicit spark: SparkSession): DatePartitions = {
    val fs = HadoopFS(root)
    DatePartitions(fs, pathGenerator)
  }

  def apply(fs: FileSystem,
            pathGenerator: DatePartitionFormatter): DatePartitions =
    new DatePartitions(fs, pathGenerator)
}

class DatePartitions(fs: FileSystem, pathGenerator: DatePartitionFormatter) {

  def generatePaths(date1: LocalDateTime, date2: LocalDateTime): Seq[String] = {
    val (from, to) = {
      if (date2.isAfter(date1)) (date1, date2)
      else if (date1.isAfter(date2)) (date2, date1)
      else (date1, date1)
    }

    generatePossibleDates(from, to).par
      .map(pathGenerator.dateToPath)
      .filter(fs.pathExists)
      .seq
  }

  private def generatePossibleDates(from: LocalDateTime,
                                    to: LocalDateTime): Seq[LocalDateTime] = {
    def generateDatesForYear(year: Int): Seq[LocalDateTime] = {
      val partitionFrom = LocalDateTime.of(year, 1, 1, 0, 0)
      val partitionTo = LocalDateTime.of(year, 12, 31, 0, 0)
      datesGen(partitionFrom, partitionTo)
    }

    if (pathGenerator.isYearlyPartitioned) {
      val yearColumnName = pathGenerator.columnNames.head
      fs.listSubdirectories("/")
        .map(_.replace(s"$yearColumnName=", "").toInt)
        .flatMap(generateDatesForYear)
        .dropWhile(_.isBefore(from))
        .takeWhile(date => date.isBefore(to) || date.isEqual(to))

    } else {
      datesGen(from, to)
    }
  }

  private def datesGen(from: LocalDateTime,
                       to: LocalDateTime): List[LocalDateTime] = {
    val minimumTime = pathGenerator.minimumTemporalUnit()

    def datesGen(acc: List[LocalDateTime],
                 pointer: LocalDateTime,
                 end: LocalDateTime): List[LocalDateTime] = {
      if (pointer.isAfter(end)) acc
      else datesGen(acc :+ pointer, pointer.plus(1, minimumTime), end)
    }
    datesGen(List(), from, to)
  }
}
