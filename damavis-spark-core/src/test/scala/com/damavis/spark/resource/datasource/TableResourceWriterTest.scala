package com.damavis.spark.resource.datasource

import java.util.concurrent.atomic.AtomicInteger

import com.damavis.spark.database.{Database, DbManager, Table}
import com.damavis.spark.entities.Person
import com.damavis.spark.resource.datasource.enums.Format
import com.damavis.spark.utils.SparkTestSupport
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types._

class TableResourceWriterTest extends SparkTestSupport {
  import session.implicits._

  var db: Database = _
  val tableCount = new AtomicInteger(0)

  override def beforeAll(): Unit = {
    super.beforeAll()

    db = DbManager.useDatabase(name, forceCreation = true)
  }

  private def preparePersonsTable(): Table = {
    val schema = StructType(
      StructField("name", StringType) ::
        StructField("age", IntegerType) ::
        StructField("nationality", StringType) ::
        Nil
    )
    val tableName = s"persons${tableCount.addAndGet(1)}"

    db.prepareTable(tableName, Format.Parquet, schema)

    val tryTable = db.getTable(tableName)

    assert(tryTable.isSuccess)

    tryTable.get
  }

  "A TableResourceWriter" when {
    "there is no partitioning" should {
      "write successfully to an empty table" in {
        val table = preparePersonsTable()
        val writer = TableWriterBuilder(table).writer()

        val personDf = (Person("Orson Scott", 68, "USA") :: Nil).toDF()
        writer.write(personDf)

        val written = session.read.parquet(table.options.path)

        assert(written.count() == 1)

        val firstRow = written.collect().head
        val expectedRow = Row("Orson Scott", 68, "USA")

        assert(firstRow == expectedRow)
      }

      "apply properly append save mode" in {
        val table = preparePersonsTable()
        val writerBuilder = TableWriterBuilder(table)

        val person1 = (Person("Orson Scott", 68, "USA") :: Nil).toDF()
        writerBuilder
          .writer()
          .write(person1)

        val person2 = (Person("Wells", 79, "UK") :: Nil).toDF()
        writerBuilder
          .saveMode(SaveMode.Append)
          .writer()
          .write(person2)

        val written = session.read.parquet(table.options.path)
        assert(written.count() == 2)

        val expectedDf = (Person("Orson Scott", 68, "USA") ::
          Person("Wells", 79, "UK") ::
          Nil).toDF()
        assert(written.except(expectedDf).isEmpty)
      }

      "apply properly overwrite save mode" in {
        val table = preparePersonsTable()
        val writerBuilder = TableWriterBuilder(table)

        val person1 = (Person("Orson Scott", 68, "USA") :: Nil).toDF()
        writerBuilder
          .writer()
          .write(person1)

        val person2 = (Person("Wells", 79, "UK") :: Nil).toDF()
        writerBuilder
          .saveMode(SaveMode.Overwrite)
          .writer()
          .write(person2)

        val written = session.read.parquet(table.options.path)

        assert(written.count() == 1)
        assert(written.except(person2).isEmpty)
      }
    }
  }

}
