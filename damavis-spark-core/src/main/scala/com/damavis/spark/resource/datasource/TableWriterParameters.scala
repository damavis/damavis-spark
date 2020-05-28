package com.damavis.spark.resource.datasource

import com.damavis.spark.resource.datasource.enums._
import org.apache.spark.sql.SaveMode
import enums.OverwritePartitionBehavior._

case class TableWriterParameters(partitionedBy: Option[Seq[String]] = None,
                                 storageFormat: Format.Format = Format.Parquet,
                                 saveMode: SaveMode = SaveMode.Overwrite,
                                 overwriteBehavior: OverwritePartitionBehavior =
                                   OVERWRITE_ALL)
