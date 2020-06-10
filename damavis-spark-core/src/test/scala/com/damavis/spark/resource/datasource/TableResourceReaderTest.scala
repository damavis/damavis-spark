package com.damavis.spark.resource.datasource

import com.damavis.spark.database.{Database, DbManager}
import com.damavis.spark.utils.SparkTestSupport
import com.damavis.spark._
import com.damavis.spark.database.exceptions.TableAccessException
import com.damavis.spark.resource.Format
import org.apache.spark.sql.SaveMode

class TableResourceReaderTest extends SparkTestSupport {
  var db: Database = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    db = DbManager.useDatabase(name, forceCreation = true)
  }

  "A TableResourceReader" should {
    "read successfully an external table" in {
      val authors = dfFromAuthors(hemingway, wells)

      authors.write.parquet(s"$root/authors")

      val tryTable =
        db.getUnmanagedTable("authors", s"/$name/authors", Format.Parquet)
      assert(tryTable.isSuccess)

      val readDf = TableReaderBuilder(tryTable.get).reader().read()

      checkDataFramesEqual(readDf, authors)
    }

    "read successfully a managed table registered in the catalog" in {
      session.catalog.createTable("uk_authors",
                                  "parquet",
                                  authorsSchema,
                                  Map[String, String]())
      val authors = dfFromAuthors(dickens, wells)

      authors.write.mode(SaveMode.Overwrite).saveAsTable("uk_authors")

      val tryTable = db.getTable("uk_authors")
      assert(tryTable.isSuccess)

      val obtained = TableReaderBuilder(tryTable.get).reader().read()

      checkDataFramesEqual(obtained, authors)
    }

    "fail to read a table not yet present in the catalog" in {
      val tryTable = db.getTable("usa_authors")
      assert(tryTable.isSuccess)

      intercept[TableAccessException] {
        TableReaderBuilder(tryTable.get).reader().read()
      }
    }
  }
}
