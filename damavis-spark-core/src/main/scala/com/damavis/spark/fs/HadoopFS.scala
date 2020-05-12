package com.damavis.spark.fs

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

object HadoopFS {
  def apply(implicit spark: SparkSession): HadoopFS = new HadoopFS()
}

class HadoopFS()(implicit spark: SparkSession) extends FileSystem {
  //Spark does not guarantee that member sessionState will be maintained across versions
  private val hadoopConf = spark.sessionState.newHadoopConf()

  override def pathExists(path: String): Boolean = {
    val hdfsPath = new Path(path)

    hdfsPath.getFileSystem(hadoopConf).exists(hdfsPath)
  }
}
