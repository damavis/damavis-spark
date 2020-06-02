package com.damavis.spark.resource.datasource

import com.damavis.spark.database.{Database, DbManager, Table}
import com.damavis.spark.{dfFromAuthors, hemingway}
import com.damavis.spark.utils.SparkTestSupport
import com.damavis.spark._
import com.damavis.spark.database.exceptions.TableDefinitionException
import com.damavis.spark.resource.datasource.enums.Format

import scala.util.Try

class TableWriterBuilderTest extends SparkTestSupport {
  implicit var db: Database = _
  private var partitionedTestTable: String = _
  private var nonPartitionedTestTable: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    db = DbManager.useDatabase(name, forceCreation = true)

    val tablePartitioned = nextTable()
    partitionedTestTable = tablePartitioned.name

    val personDf = dfFromAuthors(hemingway)
    TableWriterBuilder(tablePartitioned)
      .partitionedBy("nationality")
      .withFormat(Format.Avro)
      .writer()
      .write(personDf)

    val tableNotPartitioned = nextTable()
    nonPartitionedTestTable = tableNotPartitioned.name

    TableWriterBuilder(tableNotPartitioned)
      .writer()
      .write(personDf)

  }

  "TableWriterBuilder factory method" should {
    "return actual instance according input table" in {
      val dummyTable = nextTable()

      val personDf = dfFromAuthors(hemingway)
      val dummyTableBuilder = TableWriterBuilder(dummyTable)

      dummyTableBuilder
        .writer()
        .write(personDf)

      val tryRealTable = db.getTable(dummyTable.name)
      assert(tryRealTable.isSuccess)

      val realTableBuilder = TableWriterBuilder(tryRealTable.get)

      assert(dummyTableBuilder.getClass == classOf[BasicTableWriterBuilder])
      assert(realTableBuilder.getClass == classOf[SealedTableWriterBuilder])
    }
  }

  "A SealedTableWriterBuilder" should {

    "support a definition equal than the original" in {
      val table = db.getTable(partitionedTestTable).get

      val tryCreateWriter = Try {
        TableWriterBuilder(table)
          .withFormat(Format.Avro)
          .partitionedBy("nationality")
          .writer()
      }

      assert(tryCreateWriter.isSuccess)
    }

    "check partitioning is not added" in {
      val table = db.getTable(nonPartitionedTestTable).get

      val ex = intercept[TableDefinitionException] {
        TableWriterBuilder(table)
          .partitionedBy("nationality")
      }

      assert(ex.getMessage.contains("is already defined with no partitioning"))
    }

    "check partition columns do not change" in {
      val table = db.getTable(partitionedTestTable).get

      val ex = intercept[TableDefinitionException] {
        TableWriterBuilder(table)
          .partitionedBy("nationality1234")
      }

      assert(ex.getMessage.contains("different partitioning columns"))
    }

    "check internal format does not change" in {
      val table = db.getTable(partitionedTestTable).get

      val ex = intercept[TableDefinitionException] {
        TableWriterBuilder(table)
          .withFormat(Format.Parquet)
      }

      assert(ex.getMessage.contains("defined with a different format"))
    }
  }

}
