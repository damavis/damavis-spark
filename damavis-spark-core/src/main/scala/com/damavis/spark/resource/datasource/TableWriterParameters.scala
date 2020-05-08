package com.damavis.spark.resource.datasource

import org.apache.spark.sql.SaveMode
import OverwritePartitionBehavior._
import com.damavis.spark.resource.datasource.Format._

case class TableWriterParameters(format: Format,
                                 path: String,
                                 table: String,
                                 partitionedBy: Option[Seq[String]] = None,
                                 saveMode: SaveMode = SaveMode.Overwrite,
                                 overwriteBehavior: OverwritePartitionBehavior =
                                   OVERWRITE_MATCHING)
