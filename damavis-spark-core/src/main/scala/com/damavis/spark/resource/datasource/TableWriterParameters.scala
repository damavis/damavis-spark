package com.damavis.spark.resource.datasource

import com.damavis.spark.resource.Format
import com.damavis.spark.resource.Format.Format
import com.damavis.spark.resource.datasource.OverwritePartitionBehavior._
import org.apache.spark.sql.SaveMode

case class TableWriterParameters(partitionedBy: Option[Seq[String]] = None,
                                 storageFormat: Format = Format.Parquet,
                                 saveMode: SaveMode = SaveMode.Overwrite,
                                 overwriteBehavior: OverwritePartitionBehavior =
                                   OVERWRITE_ALL)
