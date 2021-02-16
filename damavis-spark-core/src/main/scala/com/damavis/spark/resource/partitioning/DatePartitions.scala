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
    val existingPartitions: Seq[String] = fs.listSubdirectories("/")

    val (from, to) = {
      if (date2.isAfter(date1)) (date1, date2)
      else if (date1.isAfter(date2)) (date2, date1)
      else (date1, date1)
    }

    datesGen(from, to).par
      .map(pathGenerator.dateToPath)
      .filter(partitionExists(existingPartitions, _))
      .seq
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

    if (firstPartitionChecked) fs.pathExists(partition) else false
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
