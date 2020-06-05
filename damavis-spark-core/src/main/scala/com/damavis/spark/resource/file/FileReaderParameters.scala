package com.damavis.spark.resource.file

import java.time.LocalDateTime

import com.damavis.spark.resource.Format.Format
import com.damavis.spark.resource.file.partitioning.DatePartitionFormatter
import org.apache.spark.sql.SparkSession

private[resource] object FileReaderParameters {
  def apply(format: Format, path: String)(
      implicit spark: SparkSession): FileReaderParameters = {
    FileReaderParameters(format, path, DatePartitionFormatter.standard)
  }
}

private[resource] case class FileReaderParameters(
    format: Format,
    path: String,
    partitionFormatter: DatePartitionFormatter,
    from: Option[LocalDateTime] = None,
    to: Option[LocalDateTime] = None) {

  def datePartitioned: Boolean = from.isDefined && to.isDefined
}
