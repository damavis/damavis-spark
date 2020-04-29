package com.damavis.spark.resource.parquet

import com.damavis.spark.resource.{ReaderBuilder, ResourceReader}
import org.apache.spark.sql.SparkSession

object ParquetReaderBuilder {
  def apply(path: String)(implicit spark: SparkSession) =
    new ParquetReaderBuilder(path, spark)
}

class ParquetReaderBuilder(path: String, spark: SparkSession)
    extends ReaderBuilder {
  override def reader(): ResourceReader =
    new SimpleParquetReader(path, spark)

  def datePartitioned(): PartitionedParquetReaderBuilder = {
    val sparkReader = spark.read
      .option("basePath", path)
      .format("parquet")

    val params = ParquetReaderParameters(path, sparkReader, spark)
    new PartitionedParquetReaderBuilder(params)
  }
}
