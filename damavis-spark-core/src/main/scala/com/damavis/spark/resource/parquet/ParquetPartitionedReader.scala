package com.damavis.spark.resource.parquet

import com.damavis.spark.resource.{DatePaths, ResourceReader}
import org.apache.spark.sql.{DataFrame, SparkSession}

class ParquetPartitionedReader(params: ParquetReaderParameters)
    extends ResourceReader {
  override def read(): DataFrame = {
    val from = params.from.get
    val to = params.to.get

    implicit val spark: SparkSession = params.sparkSession
    params.sparkReader.load(DatePaths.generate(params.path, from, to): _*)
  }

}
