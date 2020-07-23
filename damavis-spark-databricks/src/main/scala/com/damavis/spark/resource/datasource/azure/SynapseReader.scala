package com.damavis.spark.resource.datasource.azure

import java.net.URL

import com.damavis.spark.resource.ResourceReader
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}

class SynapseReader(url: URL, query: String, tempDir: Path)(
    implicit spark: SparkSession)
    extends ResourceReader {

  override def read(): DataFrame = {
    spark.read
      .format("com.databricks.spark.sqldw")
      .option("url", url.toString)
      .option("forwardSparkAzureStorageCredentials", "true")
      .option("tempDir", tempDir.toString)
      .option("query", query)
      .load()
  }
}
