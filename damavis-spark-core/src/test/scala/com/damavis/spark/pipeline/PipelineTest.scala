package com.damavis.spark.pipeline

import com.damavis.spark.database.{Database, DbManager}
import com.damavis.spark._
import com.damavis.spark.resource.datasource.{
  TableReaderBuilder,
  TableWriterBuilder
}
import com.damavis.spark.resource.datasource.enums.Format
import com.damavis.spark.utils.SparkTestSupport
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import scala.collection.JavaConverters._

class PipelineTest extends SparkTestSupport {

  implicit var db: Database = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    db = DbManager.useDatabase(name, forceCreation = true)
  }

  "A pipeline" should {
    "accept reader and writer as stages" in {
      import com.damavis.spark.pipeline.implicits._

      val personDf = dfFromAuthors(hemingway, bradbury, dickens)
      personDf.write.parquet(s"$root/external-authors")

      val nationalitiesTable = db.getTable("nationalities").get

      val inTable =
        db.getUnmanagedTable("external_authors_table",
                             s"$root/external-authors",
                             Format.Parquet)
          .get

      val extractNationality = new PipelineStage {
        override def transform(data: DataFrame): DataFrame =
          data.select("nationality")
      }

      val countNationalities = new PipelineStage {
        override def transform(data: DataFrame): DataFrame =
          data.groupBy("nationality").count()
      }

      val source = TableReaderBuilder(inTable).reader()
      val target = TableWriterBuilder(nationalitiesTable)
        .saveMode(SaveMode.Overwrite)
        .writer()

      source | extractNationality -> countNationalities -> target

      val written = TableReaderBuilder(db.getTable("nationalities").get)
        .reader()
        .read()

      val expectedDf = session.createDataFrame(
        (Row("USA", 2L) :: Row("UK", 1L) :: Nil).asJava,
        StructType(
          StructField("nationality", StringType, nullable = true) ::
            StructField("count", LongType, nullable = true) ::
            Nil
        )
      )

      checkDataFramesEqual(written, expectedDf)
    }
  }
}
