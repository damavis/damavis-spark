package com.damavis.spark.dataflow

import com.damavis.spark.database.{Database, DbManager}
import com.damavis.spark._
import com.damavis.spark.resource.datasource.{
  TableReaderBuilder,
  TableWriterBuilder
}
import com.damavis.spark.utils.SparkTestSupport
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import scala.language.postfixOps

class JoinPipelineTest extends SparkTestSupport {

  implicit var db: Database = _

  val authorsTableName: String = "authors"
  val booksTableName: String = "books"

  override def beforeAll(): Unit = {
    super.beforeAll()

    db = DbManager.useDatabase(name, forceCreation = true)

    val authorsTable = db.getTable(authorsTableName).get
    val authorsData = dfFromAuthors(hemingway, wells, bradbury, dickens)
    TableWriterBuilder(authorsTable).writer().write(authorsData)

    val booksTable = db.getTable(booksTableName).get
    val booksData = dfFromBooks(farewell,
                                oldMan,
                                timeMachine,
                                moreau,
                                oliverTwist,
                                expectations)
    TableWriterBuilder(booksTable).writer().write(booksData)
  }

  private val joinAuthorsProcessor = new JoinProcessor {
    override def computeImpl(left: DataFrame, right: DataFrame): DataFrame = {
      left
        .join(right, left("name") === right("author"), "inner")
        .select("author", "title", "publicationYear")
    }
  }

  private val groupByProcessor = new LinealProcessor {
    override def computeImpl(data: DataFrame): DataFrame = {
      val window = Window
        .partitionBy("author")
        .orderBy(data("publicationYear") asc)

      data
        .select(col("author"),
                col("title"),
                col("publicationYear"),
                rank().over(window) as "rank")
        .filter(col("rank") === lit(1))
        .drop("rank")
    }
  }

  "a pipeline with a join" should {
    "be processed properly" in {
      val authorsTable = db.getTable(authorsTableName).get
      val booksTable = db.getTable(booksTableName).get
      val oldestBooksTable = db.getTable("oldest_books").get

      val booksReader = TableReaderBuilder(booksTable).reader()
      val authorsReader = TableReaderBuilder(authorsTable).reader()
      val oldBookWriter = TableWriterBuilder(oldestBooksTable).writer()

      val pipeline = PipelineBuilder.create {
        implicit definition: PipelineDefinition =>
          import implicits._

          val joinStage = new PipelineStage(joinAuthorsProcessor)
          val authorOldestBook = new PipelineStage(groupByProcessor)

          authorsReader -> joinStage.left -> authorOldestBook -> oldBookWriter
          booksReader -> joinStage.right
      }

      pipeline.run()

      val generated = TableReaderBuilder(db.getTable("oldest_books").get)
        .reader()
        .read()

      val expectedData = (Row("Hemingway", "A Farewell to Arms", 1929) ::
        Row("H.G. Wells", "The Time Machine", 1895) ::
        Row("Dickens", "Oliver Twist", 1839) ::
        Nil).asJava

      val schema = StructType(
        StructField("author", StringType, nullable = true) ::
          StructField("title", StringType, nullable = true) ::
          StructField("publicationYear", IntegerType, nullable = true) :: Nil
      )

      val expected = session.createDataFrame(expectedData, schema)
      checkDataFramesEqual(generated, expected)
    }
  }
}
