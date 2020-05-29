package com.damavis.spark.resource.datasource

import java.util.concurrent.atomic.AtomicInteger

import com.damavis.spark.database.{Database, DbManager, Table}
import com.damavis.spark.resource.datasource.enums.OverwritePartitionBehavior
import com.damavis.spark.utils.SparkTestSupport
import org.apache.spark.sql.SaveMode
import com.damavis.spark._

class TableResourceWriterTest extends SparkTestSupport {

  implicit var db: Database = _
  val tableCount = new AtomicInteger(0)

  override def beforeAll(): Unit = {
    super.beforeAll()

    db = DbManager.useDatabase(name, forceCreation = true)
  }

  private def nextTable: Table = {
    val tableName = s"authors${tableCount.addAndGet(1)}"

    val tryTable = db.getTable(tableName)

    assert(tryTable.isSuccess)

    tryTable.get
  }

  "A TableResourceWriter" when {
    "there is no partitioning" should {
      "write successfully to an empty table" in {
        val table = nextTable
        val writer = TableWriterBuilder(table).writer()

        val personDf = dfFromAuthors(hemingway)
        writer.write(personDf)

        val written = session.read.table(table.name)

        assert(written.count() == 1)
        checkDataFramesEqual(written, personDf)
      }

      "apply properly append save mode" in {
        val table = nextTable
        val writerBuilder = TableWriterBuilder(table)

        val author1 = dfFromAuthors(hemingway)
        writerBuilder
          .writer()
          .write(author1)

        val author2 = dfFromAuthors(wells)
        writerBuilder
          .saveMode(SaveMode.Append)
          .writer()
          .write(author2)

        val written = session.read.table(table.name)
        assert(written.count() == 2)

        val expectedDf = dfFromAuthors(hemingway, wells)
        checkDataFramesEqual(written, expectedDf)
      }

      "apply properly overwrite save mode" in {
        val table = nextTable
        val writerBuilder = TableWriterBuilder(table)

        val author1 = dfFromAuthors(hemingway)
        writerBuilder
          .writer()
          .write(author1)

        val author2 = dfFromAuthors(wells)
        writerBuilder
          .saveMode(SaveMode.Overwrite)
          .writer()
          .write(author2)

        val written = session.read.table(table.name)

        assert(written.count() == 1)
        checkDataFramesEqual(written, author2)
      }
    }

    "there is partitioning" should {
      "write successfully to an empty table" in {
        val table = nextTable
        val writer = TableWriterBuilder(table)
          .partitionedBy("nationality")
          .writer()

        val personDf = dfFromAuthors(hemingway)
        writer.write(personDf)

        val written = session.read.table(table.name)

        assert(written.count() == 1)
        checkDataFramesEqual(written, personDf)
      }

      "overwrite all partitions by default" in {
        val table = nextTable
        val writer = TableWriterBuilder(table)
          .partitionedBy("nationality")
          .writer()

        val authors = dfFromAuthors(hemingway, wells)
        writer.write(authors)

        val anotherUSAAuthor = dfFromAuthors(bradbury)
        writer.write(anotherUSAAuthor)

        val finalDf = session.read.table(table.name)

        assert(finalDf.count() == 1)
        checkDataFramesEqual(finalDf, anotherUSAAuthor)
      }

      "overwrite all partitions if told so" in {
        val table = nextTable
        val writer = TableWriterBuilder(table)
          .partitionedBy("nationality")
          .overwritePartitionBehavior(OverwritePartitionBehavior.OVERWRITE_ALL)
          .writer()

        val authors = dfFromAuthors(hemingway, wells)
        writer.write(authors)

        val anotherUSAAuthor = dfFromAuthors(bradbury)
        writer.write(anotherUSAAuthor)

        val finalDf = session.read.table(table.name)

        assert(finalDf.count() == 1)
        checkDataFramesEqual(finalDf, anotherUSAAuthor)
      }

      "overwrite only matching partitions if parameter is set" in {
        val table = nextTable
        val writer = TableWriterBuilder(table)
          .partitionedBy("nationality")
          .overwritePartitionBehavior(
            OverwritePartitionBehavior.OVERWRITE_MATCHING)
          .writer()

        val authors = dfFromAuthors(hemingway, wells)
        writer.write(authors)

        val anotherUSAAuthor = dfFromAuthors(bradbury)
        writer.write(anotherUSAAuthor)

        val finalDf = session.read.table(table.name)

        val expectedAuthors = dfFromAuthors(bradbury, wells)

        assert(finalDf.count() == 2)
        checkDataFramesEqual(finalDf, expectedAuthors)
      }

      "do not overwrite anything if save mode is different than overwrite" in {
        val table = nextTable
        val writer = TableWriterBuilder(table)
          .partitionedBy("nationality")
          .saveMode(SaveMode.Append)
          .writer()

        val authors = dfFromAuthors(hemingway, wells)
        writer.write(authors)

        val anotherUSAAuthor = dfFromAuthors(bradbury)
        writer.write(anotherUSAAuthor)

        val finalDf = session.read.table(table.name)

        val expectedAuthors = dfFromAuthors(hemingway, wells, bradbury)

        assert(finalDf.count() == 3)
        checkDataFramesEqual(finalDf, expectedAuthors)
      }
    }
  }

}
