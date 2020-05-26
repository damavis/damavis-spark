package com.damavis.spark.resource.datasource

import java.util.concurrent.atomic.AtomicInteger

import com.damavis.spark.database.{Database, DbManager, Table}
import com.damavis.spark.entities.Person
import com.damavis.spark.resource.datasource.enums.{
  Format,
  OverwritePartitionBehavior
}
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

  private def preparePersonsTable(partitionCols: Seq[String]): Table = {
    val schema = StructType(
      StructField("name", StringType) ::
        StructField("age", IntegerType) ::
        StructField("nationality", StringType) ::
        Nil
    )
    val tableName = s"persons${tableCount.addAndGet(1)}"

    db.prepareTable(tableName, Format.Parquet, schema, partitionCols)

    val tryTable = db.getTable(tableName)

    assert(tryTable.isSuccess)

    tryTable.get
  }

  private def prepareNotPartitionedTable(): Table = preparePersonsTable(Nil)
  private def preparePartitionedTable(): Table =
    preparePersonsTable("nationality" :: Nil)

  "A TableResourceWriter" when {
    "there is no partitioning" should {
      "write successfully to an empty table" in {
        val table = prepareNotPartitionedTable()
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
        val table = prepareNotPartitionedTable()
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
        val table = prepareNotPartitionedTable()
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

    "there is partitioning" should {
      "write successfully to an empty table" in {
        val table = preparePartitionedTable()
        val writer = TableWriterBuilder(table)
          .partitionedBy("nationality")
          .writer()

        val personDf = (Person("Orson Scott", 68, "USA") :: Nil).toDF()
        writer.write(personDf)

        val written = session.read.parquet(table.options.path)

        assert(written.count() == 1)

        val firstRow = written.collect().head
        val expectedRow = Row("Orson Scott", 68, "USA")

        assert(firstRow == expectedRow)
      }

      "overwrite all partitions by default" in {
        val table = preparePartitionedTable()
        val writer = TableWriterBuilder(table)
          .partitionedBy("nationality")
          .writer()

        val authors = (Person("Orson Scott", 68, "USA") ::
          Person("Wells", 79, "UK") ::
          Nil).toDF()
        writer.write(authors)

        val anotherUSAAuthor = (Person("Ray Bradbury", 91, "USA") :: Nil).toDF()
        writer.write(anotherUSAAuthor)

        val finalDf = session.read.parquet(table.options.path)

        assert(finalDf.count() == 1)
        assert(finalDf.except(anotherUSAAuthor).count() == 0)
      }

      "overwrite all partitions if told so" in {
        val table = preparePartitionedTable()
        val writer = TableWriterBuilder(table)
          .partitionedBy("nationality")
          .overwritePartitionBehavior(OverwritePartitionBehavior.OVERWRITE_ALL)
          .writer()

        val authors = (Person("Orson Scott", 68, "USA") ::
          Person("Wells", 79, "UK") ::
          Nil).toDF()
        writer.write(authors)

        val anotherUSAAuthor = (Person("Ray Bradbury", 91, "USA") :: Nil).toDF()
        writer.write(anotherUSAAuthor)

        val finalDf = session.read.parquet(table.options.path)

        assert(finalDf.count() == 1)
        assert(finalDf.except(anotherUSAAuthor).count() == 0)
      }

      "overwrite only matching partitions if parameter is set" in {
        val table = preparePartitionedTable()
        val writer = TableWriterBuilder(table)
          .partitionedBy("nationality")
          .overwritePartitionBehavior(
            OverwritePartitionBehavior.OVERWRITE_MATCHING)
          .writer()

        val authors = (Person("Orson Scott", 68, "USA") ::
          Person("Wells", 79, "UK") ::
          Nil).toDF()
        writer.write(authors)

        val anotherUSAAuthor = (Person("Ray Bradbury", 91, "USA") :: Nil).toDF()
        writer.write(anotherUSAAuthor)

        val finalDf = session.read.parquet(table.options.path)

        val expectedAuthors = (Person("Ray Bradbury", 91, "USA") ::
          Person("Wells", 79, "UK") ::
          Nil).toDF()

        assert(finalDf.count() == 2)
        assert(finalDf.except(expectedAuthors).count() == 0)
      }

      "do not overwrite anything if save mode is different than overwrite" in {
        val table = preparePartitionedTable()
        val writer = TableWriterBuilder(table)
          .partitionedBy("nationality")
          .saveMode(SaveMode.Append)
          .writer()

        val authors = (Person("Orson Scott", 68, "USA") ::
          Person("Wells", 79, "UK") ::
          Nil).toDF()
        writer.write(authors)

        val anotherUSAAuthor = (Person("Ray Bradbury", 91, "USA") :: Nil).toDF()
        writer.write(anotherUSAAuthor)

        val finalDf = session.read.parquet(table.options.path)

        val expectedAuthors = (Person("Orson Scott", 68, "USA") ::
          Person("Wells", 79, "UK") ::
          Person("Ray Bradbury", 91, "USA") ::
          Nil).toDF()

        assert(finalDf.count() == 3)
        assert(finalDf.except(expectedAuthors).count() == 0)
      }
    }
  }

}
