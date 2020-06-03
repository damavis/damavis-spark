package com.damavis.spark.resource.datasource

import com.damavis.spark.database.{Database, DbManager}
import com.damavis.spark.resource.datasource.enums.{
  Format,
  OverwritePartitionBehavior
}
import com.damavis.spark.utils.SparkTestSupport
import org.apache.spark.sql.{Row, SaveMode}
import com.damavis.spark._
import com.damavis.spark.database.exceptions.TableAccessException
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

class TableResourceWriterTest extends SparkTestSupport {

  implicit var db: Database = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    db = DbManager.useDatabase(name, forceCreation = true)
  }

  "A TableResourceWriter" when {
    "trying to write to a table that does not exist in catalog" should {
      "table should be created automatically" in {
        val table = nextTable()
        val tableName = table.name
        val before = session.catalog.listTables().count

        val personDf = dfFromAuthors(hemingway)
        TableWriterBuilder(table)
          .writer()
          .write(personDf)

        val after = session.catalog.listTables().count()

        assert((before + 1) == after)

        val realTable = db.getTable(tableName)
        assert(realTable.get.managed)
      }
    }

    "write successfully to an external table" in {
      val personDf = dfFromAuthors(hemingway)
      personDf.write.parquet(s"$root/person")

      val tableName = "myAuthorsTable"
      val table =
        db.getUnmanagedTable(tableName, s"$root/person", Format.Parquet).get

      val before = session.catalog.listTables().count

      TableWriterBuilder(table)
        .writer()
        .write(personDf)

      val after = session.catalog.listTables().count
      assert(before == after)

      val written = session.read.table(tableName)
      checkDataFramesEqual(written, personDf)
    }

    "there is no partitioning" should {
      "write successfully to an empty table" in {
        val table = nextTable()
        val writer = TableWriterBuilder(table).writer()

        val personDf = dfFromAuthors(hemingway)
        writer.write(personDf)

        val written = session.read.table(table.name)

        assert(written.count() == 1)
        checkDataFramesEqual(written, personDf)
      }

      "apply properly append save mode" in {
        val table = nextTable()
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
        val table = nextTable()
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
        val table = nextTable()
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
        val table = nextTable()
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
        val table = nextTable()
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
        val table = nextTable()
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
        assert(finalDf.count() == 2)

        val expectedAuthors = dfFromAuthors(bradbury, wells)
        checkDataFramesEqual(finalDf, expectedAuthors)
      }

      "do not overwrite anything if save mode is different than overwrite" in {
        val table = nextTable()
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

    "trying to write a DataFrame" should {
      "do not allow write if DataFrame schema does not match table's one" in {
        val table = nextTable()
        val writer = TableWriterBuilder(table)
          .partitionedBy("nationality")
          .writer()

        val authors = dfFromAuthors(hemingway, wells)
        writer.write(authors)

        val anotherSchema = StructType(
          StructField("name", StringType, nullable = true) ::
            StructField("nationality", StringType, nullable = true) ::
            StructField("deceaseAge", IntegerType, nullable = true) ::
            Nil
        )

        val dickensList = (Row(dickens.name,
                               dickens.nationality,
                               dickens.deceaseAge) :: Nil).asJava
        val newDf = session.createDataFrame(dickensList, anotherSchema)

        val ex = intercept[TableAccessException] {
          writer.write(newDf)
        }

        assert(
          ex.getMessage.contains("does not have columns in required order"))
      }
    }
  }

}
