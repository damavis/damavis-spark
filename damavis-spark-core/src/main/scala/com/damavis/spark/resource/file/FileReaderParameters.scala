package com.damavis.spark.resource.file

import java.time.LocalDateTime

import com.damavis.spark.resource.Format.Format
import com.damavis.spark.resource.partitioning.DatePartitionFormatter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

private[resource] object FileReaderParameters {

  def apply(format: Format, path: String)(
      implicit spark: SparkSession): FileReaderParameters = {
    new FileReaderParameters(format, path, DatePartitionFormatter.standard)
  }

  def apply(format: Format,
            path: String,
            partitionFormatter: DatePartitionFormatter)(
      implicit spark: SparkSession): FileReaderParameters = {
    new FileReaderParameters(format, path, partitionFormatter)
  }

  def apply(format: Format,
            path: String,
            partitionFormatter: DatePartitionFormatter,
            options: Map[String, String])(
      implicit spark: SparkSession): FileReaderParameters = {
    new FileReaderParameters(format,
                             path,
                             partitionFormatter,
                             options = options)
  }

}

private[resource] case class FileReaderParameters(
    format: Format,
    path: String,
    partitionFormatter: DatePartitionFormatter,
    from: Option[LocalDateTime] = None,
    to: Option[LocalDateTime] = None,
    options: Map[String, String] = Map(),
    schema: Option[StructType] = None) {

  def datePartitioned: Boolean = from.isDefined && to.isDefined
}
