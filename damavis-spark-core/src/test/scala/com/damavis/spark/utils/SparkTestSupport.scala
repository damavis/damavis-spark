package com.damavis.spark.utils

import java.io.File
import java.nio.file.Files

import scala.reflect.io.Directory
import com.damavis.spark.SparkApp
import com.holdenkarau.spark.testing.HDFSCluster
import org.scalatest.{BeforeAndAfterAll, WordSpec}

class SparkTestSupport extends WordSpec with SparkApp with BeforeAndAfterAll {
  override val name: String = this.getClass.getName.split("\\.").last

  override def conf: Map[String, String] = {

    super.conf + (
      //Where to store managed tables
      "spark.sql.warehouse.dir" -> warehouseConf,
      //URI of the hdfs server
      "spark.hadoop.fs.default.name" -> hdfsCluster.getNameNodeURI(),
      //Hive metastore configuration
      "spark.sql.catalogImplementation" -> "hive",
      "spark.hadoop.javax.jdo.option.ConnectionDriverName" -> "org.apache.derby.jdbc.EmbeddedDriver",
      "spark.hadoop.javax.jdo.option.ConnectionURL" -> "jdbc:derby:memory:myInMemDB;create=true"
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
