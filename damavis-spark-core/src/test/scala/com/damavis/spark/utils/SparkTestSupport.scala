package com.damavis.spark.utils

import com.damavis.spark.SparkApp
import org.apache.spark.sql.Dataset
import org.scalatest.{BeforeAndAfterAll, WordSpec}

import scala.util.Try

class SparkTestSupport extends WordSpec with SparkApp with BeforeAndAfterAll {
  override val name: String = this.getClass.getName.split("\\.").last

  protected def root: String =
    s"${HDFSCluster.uri}/$name"

  override def conf: Map[String, String] = {

    super.conf + (
      //Where to store managed tables
      "spark.sql.warehouse.dir" -> warehouseConf,
      //URI of the hdfs server
      "spark.hadoop.fs.default.name" -> HDFSCluster.uri,
      //Hive metastore configuration
      "spark.sql.catalogImplementation" -> "hive",
      "spark.hadoop.javax.jdo.option.ConnectionDriverName" -> "org.apache.derby.jdbc.EmbeddedDriver",
      "spark.hadoop.javax.jdo.option.ConnectionURL" -> "jdbc:derby:memory:myInMemDB;create=true"
    )
  }

  private var warehouseConf: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    warehouseConf = s"/sparktest-$name-warehouse"
  }

  def checkExceptionOfType[T <: Throwable](tryResult: Try[_],
                                           exClass: Class[T],
                                           pattern: String): Unit = {
    assert(tryResult.isFailure)

    try {
      throw tryResult.failed.get
    } catch {
      case ex: Throwable =>
        assert(ex.getClass == exClass)
        assert(ex.getMessage.contains(pattern))
    }
  }

  def checkDataFramesEqual[T](obtained: Dataset[T],
                              expected: Dataset[T]): Unit = {
    if (obtained.schema != expected.schema)
      assert(false)

    assert(obtained.except(expected).isEmpty)
  }
}
