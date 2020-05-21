package com.damavis.spark.database

import com.damavis.spark.entities.Person
import com.damavis.spark.resource.datasource.enums._
import com.damavis.spark.utils.SparkTestSupport

import scala.util.Try

class DatabaseTest extends SparkTestSupport {

  import spark.implicits._

  var db: Database = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    db = DbManager.useDatabase("test", forceCreation = true)
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

    "report whether a table exists or not" in {
      assert(db.tableExists("persons"))
      assert(!db.tableExists("persons2"))

      assert(db.tableExists("test.persons"))
      assert(!db.tableExists("another_test.persons"))
    }

    "recover specified table along with its metadata" in {
      val table = db.getTable("persons").get

      // "path" field contains the whole path within the warehouse
      // This value is non-deterministic: it changes each time the test is executed
      // We will check that the table path in the database is as expected, since this is always the same
      assert(table.options.path.endsWith("/test.db/persons"))

      val cleared =
        table.copy(options = table.options.copy(path = "/test.db/persons"))
      val expected =
        Table("test",
              "persons",
              TableOptions("/test.db/persons", Format.Parquet, managed = true))

      assert(cleared == expected)
    }

    "recover table successfully if database is in table name" in {
      assert(db.getTable("test.persons").isSuccess)
    }

    "fail to recover a table that belongs to another database" in {
      assert(db.getTable("another_test.persons").isFailure)
    }

    "fail to recover a table that does not exists" in {
      assert(db.getTable("persons2").isFailure)
    }

  }
}
