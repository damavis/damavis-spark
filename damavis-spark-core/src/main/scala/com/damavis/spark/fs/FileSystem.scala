package com.damavis.spark.fs

trait FileSystem {
  def pathExists(path: String): Boolean
}
