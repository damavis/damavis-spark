package com.damavis.spark.database

import com.damavis.spark.entities.Person
import com.damavis.spark.resource.datasource.enums._
import com.damavis.spark.utils.SparkTestSupport
import com.holdenkarau.spark.testing.HDFSCluster

import scala.util.Try

class DatabaseTest extends SparkTestSupport {

  import spark.implicits._

  var db: Database = _
  var hdfsCluster: HDFSCluster = _

  override def conf: Map[String, String] = {
    super.conf + ("spark.hadoop.fs.default.name" -> hdfsCluster
      .getNameNodeURI())
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    hdfsCluster = new HDFSCluster
    hdfsCluster.startHDFS()

    db = DbManager.useDatabase("test", forceCreation = true)
  }

  override def afterAll(): Unit = {
    hdfsCluster.shutdownHDFS()

    super.afterAll()
  }

  "A database" should {
    "create a table with specified parameters" in {
      val personDf = (Person("Person", 24) :: Nil).toDF()
      val schema = personDf.schema

      db.prepareTable("persons", Format.Parquet, schema, repair = false)

      val tryTable = Try {
        db.catalog.getTable("persons")
      }

      assert(tryTable.isSuccess)
    }

    "report whether a table exists or not" in {}

    "recover specified table along with its metadata" in {}

    "recover table successfully ignoring database from table name" in {}

    "fail to recover a table that does not exists" in {}

    "fail to recover a table that belongs to another database" in {}
  }
}
