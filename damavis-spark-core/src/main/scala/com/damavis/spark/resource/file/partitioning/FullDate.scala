package com.damavis.spark.resource.file.partitioning

import java.time.LocalDateTime

import com.damavis.spark.fs.FileSystem

class FullDate(fs: FileSystem) extends DatePartitionFormat(fs) {
  override def dateToPath(date: LocalDateTime): String = ???
}
