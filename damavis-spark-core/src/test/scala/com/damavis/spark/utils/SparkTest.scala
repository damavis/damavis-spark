package com.damavis.spark.utils

import java.io.File
import java.nio.file.Files

import scala.reflect.io.Directory
import com.damavis.spark.SparkApp
import org.scalatest.{BeforeAndAfterAll, WordSpec}

import scala.collection.mutable

class SparkTest extends WordSpec with SparkApp with BeforeAndAfterAll {
  override val name: String = this.getClass.getName

  override def conf: Map[String, String] = {
    warehouseConf.foldLeft(Map[String, String]())((imm, kv) => imm + kv)
  }

  private val warehouseConf = mutable.Map.empty[String, String]

  override def beforeAll(): Unit = {
    super.beforeAll()

    val className = name.split("\\.").last
    val warehousePath =
      Files.createTempDirectory(s"sparktest-$className-warehouse-")

    warehouseConf += ("spark.sql.warehouse.dir" -> warehousePath.toString)
  }

  override def afterAll(): Unit = {
    val path = warehouseConf("spark.sql.warehouse.dir")
    val directory = new Directory(new File(path))

    directory.deleteRecursively()

    super.afterAll()
  }
}
