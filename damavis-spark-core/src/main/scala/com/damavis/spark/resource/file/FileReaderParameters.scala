package com.damavis.spark.resource.file

import java.time.LocalDateTime
import com.damavis.spark.resource.Format.Format
import org.apache.spark.sql.SparkSession

private[resource] case class FileReaderParameters(
    format: Format,
    path: String,
    sparkSession: SparkSession,
    partitioningFormat: DatePartitionFormat = new DateSplitPartitioning,
    from: Option[LocalDateTime] = None,
    to: Option[LocalDateTime] = None) {

  def datePartitioned: Boolean = from.isDefined && to.isDefined
}
