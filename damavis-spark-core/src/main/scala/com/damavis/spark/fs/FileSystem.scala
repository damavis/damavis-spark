package com.damavis.spark.fs

trait FileSystem {
  def pathExists(path: String): Boolean
  def listSubdirectories(path: String): Seq[String]
}
