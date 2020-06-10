package com.damavis.spark.resource.partitioning

import java.time.LocalDateTime

import com.damavis.spark.fs.{FileSystem, HadoopFS}
import org.apache.spark.sql.SparkSession

import scala.reflect.io.Path

object DatePartitions {
  def apply(root: String, pathGenerator: DatePartitionFormatter)(
      implicit spark: SparkSession): DatePartitions = {
    val fs = HadoopFS(root)
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
