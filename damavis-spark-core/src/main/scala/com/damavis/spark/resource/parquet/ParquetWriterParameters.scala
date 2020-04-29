package com.damavis.spark.resource.parquet

case class ParquetWriterParameters(path: String,
                                   mode: String = "overwrite",
                                   columnNames: Seq[String] = Nil)
