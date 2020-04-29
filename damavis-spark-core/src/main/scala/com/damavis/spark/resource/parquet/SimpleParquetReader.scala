package com.damavis.spark.resource.parquet

import com.damavis.spark.resource.ResourceReader
import org.apache.spark.sql.{DataFrame, SparkSession}

class SimpleParquetReader(path: String, spark: SparkSession)
    extends ResourceReader {
  override def read(): DataFrame = spark.read.parquet(path)
}
