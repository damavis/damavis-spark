package com.damavis.spark.resource.file

import com.damavis.spark.fs.{FileSystem, HadoopFS}
import com.damavis.spark.resource.{DatePaths, ResourceReader}
import org.apache.spark.sql.{DataFrame, SparkSession}

class FileReader(params: FileReaderParameters) extends ResourceReader {
  private implicit val fs: FileSystem = HadoopFS()(params.sparkSession)

  override def read(): DataFrame = {
    implicit val spark: SparkSession = params.sparkSession
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
