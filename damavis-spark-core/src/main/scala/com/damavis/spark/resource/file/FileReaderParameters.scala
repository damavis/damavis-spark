package com.damavis.spark.resource.file

import java.time.LocalDateTime

import com.damavis.spark.resource.Format.Format
import com.damavis.spark.resource.file.partitioning.{
  DateSplit,
  PartitionDateFormatter
}
import org.apache.spark.sql.SparkSession

private[resource] object FileReaderParameters {
  def apply(format: Format, path: String)(
      implicit spark: SparkSession): FileReaderParameters = {
    FileReaderParameters(format, path, new DateSplit)
  }
}

private[resource] case class FileReaderParameters(
    format: Format,
    path: String,
    partitioningFormat: PartitionDateFormatter,
    from: Option[LocalDateTime] = None,
    to: Option[LocalDateTime] = None) {

  def datePartitioned: Boolean = from.isDefined && to.isDefined
}
