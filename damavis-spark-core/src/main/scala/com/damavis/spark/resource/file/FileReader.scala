package com.damavis.spark.resource.file

import com.damavis.spark.resource.{DatePaths, ResourceReader}
import org.apache.spark.sql.{DataFrame, SparkSession}

class FileReader(params: FileReaderParameters)(implicit spark: SparkSession)
    extends ResourceReader {
  override def read(): DataFrame = {
    val path = params.path

    if (params.datePartitioned) {
      val from = params.from.get
      val to = params.to.get

      val partitionsToLoad = params.partitioningFormat
        .generatePaths(from, to)
        .map(partition => s"$path/$partition")

      spark.read
        .option("basePath", path)
        .format(params.format.toString)
        //.load(DatePaths.generate(path, from, to): _*)
        .load(partitionsToLoad: _*)

    } else {
      spark.read
        .format(params.format.toString)
        .load(path)
    }
  }
}
