package com.damavis.spark.resource.file.partitioning

import java.time.LocalDateTime

import com.damavis.spark.fs.{FileSystem, HadoopFS}
import org.apache.spark.sql.SparkSession

object DatePartitions {
  def apply(pathGenerator: DatePartitionFormatter)(
      implicit spark: SparkSession): DatePartitions = {
    val fs = HadoopFS()
    new DatePartitions(fs, pathGenerator)
  }
}

class DatePartitions(fs: FileSystem, pathGenerator: DatePartitionFormatter) {
  def generatePaths(from: LocalDateTime, to: LocalDateTime): Seq[String] = {
    datesGen(from, to).par
      .map(pathGenerator.dateToPath)
      .filter(fs.pathExists)
      .seq
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
