package com.damavis.spark.utils

import java.io.File
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicReference

import scala.reflect.io.Directory
import com.damavis.spark.SparkApp
import org.scalatest.{BeforeAndAfterAll, WordSpec}

class SparkTestSupport extends WordSpec with SparkApp with BeforeAndAfterAll {
  override val name: String = this.getClass.getName

  override def conf: Map[String, String] = {
    super.conf + ("spark.sql.warehouse.dir" -> warehouseConf.get())
  }

  private val warehouseConf = new AtomicReference[String]

  override def beforeAll(): Unit = {
    super.beforeAll()

    val className = name.split("\\.").last
    val warehousePath =
      Files.createTempDirectory(s"sparktest-$className-warehouse-")

    warehouseConf.set(warehousePath.toString)
  }

  override def afterAll(): Unit = {
    val path = warehouseConf.get()
    val directory = new Directory(new File(path))

    directory.deleteRecursively()

    super.afterAll()
  }
}
