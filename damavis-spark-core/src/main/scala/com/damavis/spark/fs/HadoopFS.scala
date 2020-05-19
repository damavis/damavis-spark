package com.damavis.spark.fs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

object HadoopFS {
  def apply(implicit spark: SparkSession): HadoopFS = new HadoopFS()
}

class HadoopFS()(implicit spark: SparkSession) extends FileSystem {

  protected val hadoopConf: Configuration =
    spark.sparkContext.hadoopConfiguration

  override def pathExists(path: String): Boolean = {
    val hdfsPath = new Path(path)

    hdfsPath.getFileSystem(hadoopConf).exists(hdfsPath)
  }
}
