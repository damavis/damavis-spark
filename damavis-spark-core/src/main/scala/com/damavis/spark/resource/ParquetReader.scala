package com.damavis.spark.resource

import org.apache.spark.sql.{DataFrame, SparkSession}

object ParquetReader {
  def apply(path: String)(implicit spark: SparkSession): ParquetReader = {
    new ParquetReader(path, spark)
  }
}

class ParquetReader(path: String, spark: SparkSession) extends ResourceReader {
  override def read(): DataFrame = spark.read.parquet(path)

  def datePartitioned(): ParquetPartitionedReader =
    ParquetPartitionedReader(path, spark)
}
