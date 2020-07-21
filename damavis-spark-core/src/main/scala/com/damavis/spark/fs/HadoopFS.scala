package com.damavis.spark.fs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

object HadoopFS {
  def apply(root: String = "/")(implicit spark: SparkSession): HadoopFS =
    new HadoopFS(new Path(root))
}

class HadoopFS(root: Path)(implicit spark: SparkSession) extends FileSystem {

  protected def hadoopConf: Configuration =
    spark.sessionState.newHadoopConf()

  override def pathExists(path: String): Boolean = {
    val hdfsPath = new Path(s"$root/$path")

    val fs = hdfsPath.getFileSystem(hadoopConf)
    fs.exists(hdfsPath)
  }
}
