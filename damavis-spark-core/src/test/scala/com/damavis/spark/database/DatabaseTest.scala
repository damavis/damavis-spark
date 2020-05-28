package com.damavis.spark.database

import com.damavis.spark.database.exceptions.TableAccessException
import com.damavis.spark.entities.Person
import com.damavis.spark.resource.datasource.enums._
import com.damavis.spark.utils.SparkTestSupport
import org.apache.spark.sql.functions._

import scala.util.Try

class DatabaseTest extends SparkTestSupport {

  import session.implicits._

  var db: Database = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    db = DbManager.useDatabase("test", forceCreation = true)
  }

  "A database" should {
    "get an external table" in {
      val numbersDf = (1 :: 2 :: 3 :: 4 :: Nil).toDF("number")

      numbersDf.write
        .parquet(s"$root/numbers_external")

      assert(db.catalog.listTables().isEmpty)

      val tryTable = db.getExternalTable("numbers",
                                         s"$root/numbers_external",
                                         Format.Parquet)
      assert(tryTable.isSuccess)

      val table = tryTable.get
      val expected = RealTable("test",
                               "numbers",
                               s"$root/numbers_external",
                               Format.Parquet,
                               managed = false)
      assert(table === expected)

      assert(db.catalog.listTables().count() == 1)
    }

    "fail to get an external table if there is no data" in {
      val tryTable =
        db.getExternalTable("numbers", s"$root/1234", Format.Parquet)
      checkExceptionOfType[TableAccessException](tryTable, "Path not reachable")
    }

    "fail to get an external table if validations do not succeed" in {
      val numbersDf = (1 :: 2 :: 3 :: 4 :: Nil).toDF("number")

      db.catalog.createTable("numbers1",
                             "parquet",
                             numbersDf.schema,
                             Map[String, String]())

      val tryTable1 = db.getExternalTable("numbers1",
                                          s"$root/numbers_external",
                                          Format.Parquet)
      checkExceptionOfType[TableAccessException](
        tryTable1,
        "already registered as MANAGED")

      //Register an external table, and try to get it again but with wrong parameters
      numbersDf.write
        .parquet(s"$root/numbers_external2")

      db.getExternalTable("numbers_wrong_path",
                          s"$root/numbers_external2",
                          Format.Parquet)

      val tryTable2 = db.getExternalTable("numbers_wrong_path",
                                          s"$root/numbers_external",
                                          Format.Parquet)
      checkExceptionOfType[TableAccessException](
        tryTable2,
        "It is already registered in the catalog with a different path")

      val tryTable3 = db.getExternalTable("numbers_wrong_path",
                                          s"$root/numbers_external2",
                                          Format.Avro)
      checkExceptionOfType[TableAccessException](
        tryTable3,
        "It is already registered in the catalog with format")
    }

    "create a managed table from a DummyTable specification" in {
      val dummy = DummyTable("test", "dummy_going_real")
      val schema = (1 :: 2 :: 3 :: 4 :: Nil).toDF("number").schema

      val obtained = db.addTableIfNotExists(dummy, schema, Format.Parquet, Nil)
      val expected = RealTable(
        "test",
        "dummy_going_real",
        s"$root/sparktest-DatabaseTest-warehouse/test.db/dummy_going_real",
        Format.Parquet,
        managed = true)

      assert(obtained === expected)

      val tableExists = db.catalog
        .listTables()
        .filter(col("name") === lit("dummy_going_real"))
        .count == 1
      assert(tableExists)
    }

    "call to addTableIfNotExists should do nothing if table already exists" in {
      val before = db.catalog.listTables().count

      val dummy = DummyTable("test", "dummy_going_real")
      val schema = (1 :: 2 :: 3 :: 4 :: Nil).toDF("number").schema

      db.addTableIfNotExists(dummy, schema, Format.Parquet, Nil)

      val after = db.catalog.listTables().count()

      assert(before == after)
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
      // We will check that the table path within the database is as expected, since this is always the same
      assert(table.path.endsWith("/test.db/persons"))

      /*val cleared =
        table.copy(options = table.options.copy(path = "/test.db/persons"))
      val expected =
        Table("test",
              "persons",
              TableOptions("/test.db/persons", Format.Parquet, managed = true))

      assert(cleared == expected) */
    }

    "get table successfully if database is in table name" in {
      assert(db.getTable("test.persons").isSuccess)
    }

    "fail to get a table that belongs to another database" in {
      assert(db.getTable("another_test.persons").isFailure)
    }

    "fail to get a table that does not exists" in {
      assert(db.getTable("persons2").isFailure)
    }

  }
}
