package com.damavis.spark.resource.datasource.azure

import java.net.URL

import com.damavis.spark.resource.ResourceWriter
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}

class SynapseWriter(url: URL, table: String, tempDir: Path)(implicit spark: SparkSession)
  extends ResourceWriter {

  override def write(data: DataFrame): Unit = {
    data.write
      .format("com.databricks.spark.sqldw")
      .option("url", url.toString)
      .option("forwardSparkAzureStorageCredentials", "true")
      .option("tempDir", tempDir.toString)
      .option("dbTable", table)
      .save()
  }

}
