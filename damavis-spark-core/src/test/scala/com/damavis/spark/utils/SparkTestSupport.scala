package com.damavis.spark.utils

import java.io.File
import java.nio.file.Files

import scala.reflect.io.Directory
import com.damavis.spark.SparkApp
import com.holdenkarau.spark.testing.HDFSCluster
import org.scalatest.{BeforeAndAfterAll, WordSpec}

class SparkTestSupport extends WordSpec with SparkApp with BeforeAndAfterAll {
  override val name: String = this.getClass.getName

  override def conf: Map[String, String] = {
    super.conf + (
      "spark.sql.warehouse.dir" -> warehouseConf,
      "spark.hadoop.fs.default.name" -> hdfsCluster.getNameNodeURI()
    )
  }

  private var warehouseConf: String = _
  protected var hdfsCluster: HDFSCluster = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val className = name.split("\\.").last
    val warehousePath =
      Files.createTempDirectory(s"sparktest-$className-warehouse-")

    warehouseConf = warehousePath.toString

    hdfsCluster = new HDFSCluster
    hdfsCluster.startHDFS()
  }

  override def afterAll(): Unit = {
    hdfsCluster.shutdownHDFS()

    val directory = new Directory(new File(warehouseConf))

    directory.deleteRecursively()

    super.afterAll()
  }
}
