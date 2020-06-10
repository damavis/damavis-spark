package com.damavis.spark.database

import com.damavis.spark.database.exceptions.TableAccessException
import com.damavis.spark.resource.Format
import com.damavis.spark.utils.SparkTestSupport
import org.apache.spark.sql.functions._

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

      val tryTable = db.getUnmanagedTable("numbers",
                                          s"/$name/numbers_external",
                                          Format.Parquet)
      assert(tryTable.isSuccess)

      val table = tryTable.get
      val expected = RealTable("test",
                               "numbers",
                               s"/$name/numbers_external",
                               Format.Parquet,
                               managed = false,
                               Nil)
      assert(table === expected)

      assert(db.catalog.listTables().count() == 1)
    }

    "fail to get an external table if there is no data" in {
      val tryTable =
        db.getUnmanagedTable("numbers", s"/$name/1234", Format.Parquet)
      checkExceptionOfType(tryTable,
                           classOf[TableAccessException],
                           "Path not reachable")
    }

    "fail to get an external table if validations do not succeed" in {
      val numbersDf = (1 :: 2 :: 3 :: 4 :: Nil).toDF("number")

      db.catalog.createTable("numbers1",
                             "parquet",
                             numbersDf.schema,
                             Map[String, String]())

      val tryTable1 = db.getUnmanagedTable("numbers1",
                                           s"/$name/numbers_external",
                                           Format.Parquet)
      checkExceptionOfType(tryTable1,
                           classOf[TableAccessException],
                           "already registered as MANAGED")

      //Register an external table, and try to get it again but with wrong parameters
      numbersDf.write
        .parquet(s"/$name/numbers_external2")

      db.getUnmanagedTable("numbers_wrong_path",
                           s"/$name/numbers_external2",
                           Format.Parquet)

      val tryTable2 = db.getUnmanagedTable("numbers_wrong_path",
                                           s"/$name/numbers_external",
                                           Format.Parquet)
      checkExceptionOfType(
        tryTable2,
        classOf[TableAccessException],
        "It is already registered in the catalog with a different path")

      val tryTable3 = db.getUnmanagedTable("numbers_wrong_path",
                                           s"/$name/numbers_external2",
                                           Format.Avro)
      checkExceptionOfType(
        tryTable3,
        classOf[TableAccessException],
        "It is already registered in the catalog with format")
    }

    "create a managed table from a DummyTable specification" in {
      val dummy = DummyTable("test", "dummy_going_real")
      val schema = (1 :: 2 :: 3 :: 4 :: Nil).toDF("number").schema

      val obtained = db.addTableIfNotExists(dummy, schema, Format.Parquet, Nil)
      val expected = RealTable(
        "test",
        "dummy_going_real",
        "hdfs://localhost:8020/sparktest-DatabaseTest-warehouse/test.db/dummy_going_real",
        Format.Parquet,
        managed = true,
        Column("number", "int", partitioned = false, nullable = true) :: Nil
      )

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
      assert(db.tableExists("numbers"))
      assert(!db.tableExists("persons2"))

      assert(db.tableExists("test.numbers"))
      assert(!db.tableExists("another_test.persons"))
    }

    "recover properly table metadata" in {
      val obtained = db.getTable("numbers").get
      val expected = RealTable(
        "test",
        "numbers",
        s"$root/numbers_external",
        Format.Parquet,
        managed = false,
        Column("number", "int", partitioned = false, nullable = true) :: Nil)
      assert(obtained === expected)
    }

    "get table successfully if database is in table name" in {
      assert(db.getTable("test.persons").isSuccess)
    }

    "fail to get a table that belongs to another database" in {
      assert(db.getTable("another_test.persons").isFailure)
    }

    "return a DummyTable of a table that does not exists" in {
      assert(!db.tableExists("persons2"))

      val tryTable = db.getTable("persons2")
      assert(tryTable.isSuccess)

      val table = tryTable.get
      assert(table.getClass === classOf[DummyTable])
    }

  }
}
