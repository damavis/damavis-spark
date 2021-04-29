package com.damavis.spark.resource.file

import com.damavis.spark.resource.partitioning.DatePartitions
import com.damavis.spark.resource.ResourceReader
import org.apache.spark.sql.{DataFrame, SparkSession}

class FileReader(params: FileReaderParameters)(implicit spark: SparkSession)
  extends ResourceReader {

  override def read(): DataFrame = {

    val path = params.path
    val reader = {
      val reader = spark.read
        .options(params.options)
        .format(params.format.toString)

      params.schema match {
        case Some(schema) => reader.schema(schema)
        case None         => reader
      }
    }

    if (params.datePartitioned) {
      val from = params.from.get
      val to = params.to.get
      val partitionGenerator =
        DatePartitions(params.path, params.partitionFormatter)

      val partitionsToLoad = partitionGenerator
        .generatePaths(from, to)
        .map(partition => s"$path/$partition")

      reader
        .option("basePath", path)
        .load(partitionsToLoad: _*)

    } else {
      reader.load(path)
    }
  }

}
