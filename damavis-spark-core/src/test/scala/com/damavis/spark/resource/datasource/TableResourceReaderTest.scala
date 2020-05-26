package com.damavis.spark.resource.datasource

import com.damavis.spark.database.{Database, DbManager}
import com.damavis.spark.entities.Person
import com.damavis.spark.resource.datasource.enums.Format
import com.damavis.spark.utils.SparkTestSupport
import org.apache.spark.sql.Row

class TableResourceReaderTest extends SparkTestSupport {
  import session.implicits._

  var db: Database = _

  val tableName: String = "tableResourceTest"

  override def beforeAll(): Unit = {
    super.beforeAll()

    db = DbManager.useDatabase(name, forceCreation = true)

    prepareDatabase()
  }

  private def prepareDatabase(): Unit = {
    val personDf = (Person("Douglas Adams", 49, "UK") :: Nil).toDF()
    val schema = personDf.schema

    db.prepareTable(tableName, Format.Avro, schema)

    personDf.write
      .insertInto(tableName)
  }

  "A TableResourceReader" should {
    "retrieve table data" in {
      val tryTable = db.getTable(tableName)

      assert(tryTable.isSuccess)

      val table = tryTable.get
      val reader = new TableReaderBuilder(table).reader()
      val obtained = reader.read()

      assert(obtained.count() == 1)

      val firstRow = obtained.collect().head
      val expectedRow = Row("Douglas Adams", 49, "UK")

      assert(firstRow == expectedRow)
    }
  }
}
