package com.damavis.spark.resource.datasource

import org.apache.spark.sql.SaveMode

case object TableWriterParameters {
  object OverwritePartitionBehavior extends Enumeration {
    type OverwritePartitionBehavior = Value
    val OVERWRITE_ALL, OVERWRITE_MATCHING = Value
  }
}

import TableWriterParameters.OverwritePartitionBehavior._

case class TableWriterParameters(format: String,
                                 path: String,
                                 table: String,
                                 partitionedBy: Option[Seq[String]] = None,
                                 saveMode: SaveMode = SaveMode.Overwrite,
                                 overwriteBehavior: OverwritePartitionBehavior =
                                   OVERWRITE_MATCHING)
