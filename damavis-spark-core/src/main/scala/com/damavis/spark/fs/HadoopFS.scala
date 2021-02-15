package com.damavis.spark.fs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs
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

    root
      .getFileSystem(hadoopConf)
      .exists(hdfsPath)
  }

  override def listSubdirectories(path: String): Seq[String] = {
    root
      .getFileSystem(hadoopConf)
      .listStatus(root.suffix(path))
      .filter(_.isDirectory)
      .map(_.getPath.getName)
  }

}
