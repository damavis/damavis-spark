package com.damavis.spark.resource.datasource

import com.damavis.spark.database.{Database, DbManager}
import com.damavis.spark.resource.Format
import com.damavis.spark.testdata.{dfFromAuthors, hemingway}
import com.damavis.spark.utils.SparkTestBase

class DeltaTableWriterBuilderTest extends SparkTestBase {

  "DeltaTableWriterBuilder factory method" should {
    "return actual instance according input table" in {
      implicit val db: Database =
        DbManager.useDatabase("test", forceCreation = true)
      val table = db.getTable("test")

      val sink = TableWriterBuilder(table)
        .withFormat(Format.Parquet)
        .partitionedBy("nationality")
        .overwritePartitionBehavior(
          OverwritePartitionBehavior.OVERWRITE_MATCHING)
        .writer()

      val authors = dfFromAuthors(hemingway)

      sink.write(authors)

      val read = db.getTable("test")
      val written = TableReaderBuilder(read)
        .reader()

      assertDataFrameEquals(written.read(), authors)

    }
  }

}
