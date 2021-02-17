package com.damavis.spark.resource.partitioning

import java.time.{Duration, LocalDateTime}
import com.damavis.spark.fs.{FileSystem, HadoopFS}
import org.apache.spark.sql.SparkSession

import java.util.concurrent.atomic.AtomicInteger

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
  var queries: AtomicInteger = new AtomicInteger(0)

  def generatePaths(date1: LocalDateTime, date2: LocalDateTime): Seq[String] = {
    val (from, to) = {
      if (date2.isAfter(date1)) (date1, date2)
      else if (date1.isAfter(date2)) (date2, date1)
      else (date1, date1)
    }

    println(from)
    println(to)
    println(
      s"possible partitions are: ${Duration.between(from, to.plusDays(1)).toDays}")

    val toRet = generatePossibleDates(from, to).par
      .map(pathGenerator.dateToPath)
      .map { x =>
        queries.incrementAndGet(); x
      }
      .filter(fs.pathExists)
      .seq

    println(s"queries done are: $queries")
    println(s"actual ones are: ${toRet.length}")

    toRet
  }

  private def partitionExists(existingPartitions: Seq[String],
                              partition: String): Boolean = {
    val firstPartitionChecked =
      if (pathGenerator.columnNames.length > 1) { // Skip following test when only one partition is specified
        val firstColumnValue = partition.substring(0, partition.indexOf('/'))
        existingPartitions.contains(firstColumnValue)
      } else {
        true
      }

    if (firstPartitionChecked) {
      queries.getAndIncrement()
      fs.pathExists(partition)
    } else false
  }

  private def generatePossibleDates(from: LocalDateTime,
                                    to: LocalDateTime): Seq[LocalDateTime] = {
    val generateDatesForYear = (year: Int) => {
      val partitionFrom = LocalDateTime.of(year, 1, 1, 0, 0)
      val partitionTo = LocalDateTime.of(year, 12, 31, 0, 0)
      datesGen(partitionFrom, partitionTo)
    }

    if (pathGenerator.isYearlyPartitioned) {
      val yearColumnName = pathGenerator.columnNames.head
      fs.listSubdirectories("/")
        .map(_.replace(s"$yearColumnName=", "").toInt)
        .flatMap(generateDatesForYear)

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
