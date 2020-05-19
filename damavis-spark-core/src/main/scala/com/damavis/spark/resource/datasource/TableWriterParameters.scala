package com.damavis.spark.resource.datasource

import org.apache.spark.sql.SaveMode
import enums.OverwritePartitionBehavior._

case class TableWriterParameters(partitionedBy: Option[Seq[String]] = None,
                                 saveMode: SaveMode = SaveMode.Overwrite,
                                 overwriteBehavior: OverwritePartitionBehavior =
                                   OVERWRITE_ALL)
