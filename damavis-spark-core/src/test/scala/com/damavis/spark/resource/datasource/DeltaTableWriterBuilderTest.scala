package com.damavis.spark.resource.datasource

import com.damavis.spark.database.{Database, DbManager}
import com.damavis.spark.resource.Format
import com.damavis.spark.testdata.{dfFromAuthors, hemingway}
import com.damavis.spark.utils.SparkTestBase

class DeltaTableWriterBuilderTest extends SparkTestBase {

  "DeltaTableWriterBuilder factory method" should {
    "return actual instance according input table" in {

      val authors = dfFromAuthors(hemingway)

      implicit val db: Database = {
        DbManager.useDatabase("test", forceCreation = true)
      }

      val table = db.getTable("test")

      val sink = TableWriterBuilder(table)
        .withFormat(Format.Parquet)
        .partitionedBy("nationality")
        .overwritePartitionBehavior(OverwritePartitionBehavior.OVERWRITE_MATCHING)
        .writer()

      sink.write(authors)

      val read = db.getTable("test")
      val written = TableReaderBuilder(read)
        .reader()

      assertDataFrameEquals(written.read(), authors)
      // Try to merge by pk

      val table2 = db.getTable("test2")
      val sink2 = TableWriterBuilder(table2)
        .withFormat(Format.Delta)
        .pk(Seq("name"))
        .overwritePartitionBehavior(OverwritePartitionBehavior.OVERWRITE_MATCHING)
        .writer()

      sink2.write(authors)

      val read2 = db.getTable("test2")
      val written2 = TableReaderBuilder(read2)
        .reader()

      assertDataFrameEquals(written2.read(), authors)

    }
  }

}
