package com.damavis.spark.resource.parquet

import com.damavis.spark.resource.{DatePaths, ResourceReader}
import org.apache.spark.sql.{DataFrame, SparkSession}

class ParquetReader(params: ParquetReaderParameters) extends ResourceReader {
  override def read(): DataFrame = {
    implicit val spark: SparkSession = params.sparkSession
    val path = params.path

    if (params.datePartitioned) {
      val from = params.from.get
      val to = params.to.get

      val sparkReader = spark.read
        .option("basePath", path)
        .format("parquet")

      sparkReader.load(DatePaths.generate(path, from, to): _*)
    } else {
      spark.read.parquet(path)
    }
  }
}
