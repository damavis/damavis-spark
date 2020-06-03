package com.damavis.spark.resource.file

import java.time.LocalDateTime

trait DatePartitionFormat {
  def generatePaths(from: LocalDateTime, to: LocalDateTime): Seq[String]
}
