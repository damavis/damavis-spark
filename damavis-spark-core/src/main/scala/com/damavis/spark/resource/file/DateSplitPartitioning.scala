package com.damavis.spark.resource.file
import java.time.LocalDateTime

class DateSplitPartitioning extends DatePartitionFormat {
  override def generatePaths(from: LocalDateTime,
                             to: LocalDateTime): Seq[String] = ???
}
