package com.damavis.spark.resource.file.partitioning

import java.time.LocalDateTime

import com.damavis.spark.fs.{FileSystem, HadoopFS}
import org.apache.spark.sql.SparkSession

object DateSplit {
  def apply()(implicit spark: SparkSession): DateSplit = {
    new DateSplit(HadoopFS())
  }
}

class DateSplit(fs: FileSystem) extends DatePartitionFormat(fs) {
  override def dateToPath(date: LocalDateTime): String = {
    s"year=${date.getYear}/month=${date.getMonthValue}/day=${date.getDayOfMonth}"
  }
}
