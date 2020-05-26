package com.damavis.spark.resource.datasource

import java.util.concurrent.atomic.AtomicInteger

import com.damavis.spark.database.{Database, DbManager, Table}
import com.damavis.spark.resource.datasource.enums.{
  Format,
  OverwritePartitionBehavior
}
import com.damavis.spark.utils.SparkTestSupport
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._
import com.damavis.spark._

class TableResourceWriterTest extends SparkTestSupport {

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

        val personDf = dfFromAuthors(orson)
        writer.write(personDf)

        val written = session.read.parquet(table.options.path)

        assert(written.count() == 1)
        assert(written.except(personDf).isEmpty)
      }

      "apply properly append save mode" in {
        val table = prepareNotPartitionedTable()
        val writerBuilder = TableWriterBuilder(table)

        val person1 = dfFromAuthors(orson)
        writerBuilder
          .writer()
          .write(person1)

        val person2 = dfFromAuthors(wells)
        writerBuilder
          .saveMode(SaveMode.Append)
          .writer()
          .write(person2)

        val written = session.read.parquet(table.options.path)
        assert(written.count() == 2)

        val expectedDf = dfFromAuthors(orson, wells)
        assert(written.except(expectedDf).isEmpty)
      }

      "apply properly overwrite save mode" in {
        val table = prepareNotPartitionedTable()
        val writerBuilder = TableWriterBuilder(table)

        val person1 = dfFromAuthors(orson)
        writerBuilder
          .writer()
          .write(person1)

        val person2 = dfFromAuthors(wells)
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

        val personDf = dfFromAuthors(orson)
        writer.write(personDf)

        val written = session.read.parquet(table.options.path)

        assert(written.count() == 1)
        assert(written.except(personDf).isEmpty)
      }

      "overwrite all partitions by default" in {
        val table = preparePartitionedTable()
        val writer = TableWriterBuilder(table)
          .partitionedBy("nationality")
          .writer()

        val authors = dfFromAuthors(orson, wells)
        writer.write(authors)

        val anotherUSAAuthor = dfFromAuthors(bradbury)
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

        val authors = dfFromAuthors(orson, wells)
        writer.write(authors)

        val anotherUSAAuthor = dfFromAuthors(bradbury)
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

        val authors = dfFromAuthors(orson, wells)
        writer.write(authors)

        val anotherUSAAuthor = dfFromAuthors(bradbury)
        writer.write(anotherUSAAuthor)

        val finalDf = session.read.parquet(table.options.path)

        val expectedAuthors = dfFromAuthors(bradbury, wells)

        assert(finalDf.count() == 2)
        assert(finalDf.except(expectedAuthors).count() == 0)
      }

      "do not overwrite anything if save mode is different than overwrite" in {
        val table = preparePartitionedTable()
        val writer = TableWriterBuilder(table)
          .partitionedBy("nationality")
          .saveMode(SaveMode.Append)
          .writer()

        val authors = dfFromAuthors(orson, wells)
        writer.write(authors)

        val anotherUSAAuthor = dfFromAuthors(bradbury)
        writer.write(anotherUSAAuthor)

        val finalDf = session.read.parquet(table.options.path)

        val expectedAuthors = dfFromAuthors(orson, wells, bradbury)

        assert(finalDf.count() == 3)
        assert(finalDf.except(expectedAuthors).count() == 0)
      }
    }
  }

}
