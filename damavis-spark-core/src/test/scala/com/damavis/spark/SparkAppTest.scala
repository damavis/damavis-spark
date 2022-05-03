package com.damavis.spark

import com.damavis.spark.database.{Column, DbManager, DummyTable, RealTable}
import com.damavis.spark.resource.Format
import com.damavis.spark.utils.HDFSCluster
import org.scalatest.FlatSpec

class SparkAppTest extends FlatSpec with SparkApp {

  override val name: String = "SparkAppTest"

  private val warehouseDir = "/hive/warehouse/SparkAppTest"

  override def conf: Map[String, String] = super.conf + (
    // Where to store managed tables
    "spark.sql.warehouse.dir" -> warehouseDir,
    // URI of the hdfs server
    "spark.hadoop.fs.default.name" -> HDFSCluster.uri,
    // Hive metastore configuration
    "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.catalogImplementation" -> "hive"
  )

  "An SparkApp" should
    "run successfully" in {
      import spark.implicits._
      val df = spark.sparkContext.parallelize(List(1, 2, 3)).toDF("number")
      assert(df.count() === 3)
    }

  it should "create databases in defined warehouse path" in {
    import spark.implicits._
    val db = DbManager.useDatabase("test", forceCreation = true)
    val dummy = DummyTable("test", "dummy_going_real")
    val schema = (1 :: 2 :: 3 :: 4 :: Nil).toDF("number").schema
    val obtained = db.addTableIfNotExists(dummy, schema, Format.Parquet, Nil)

    val expected = RealTable(
      "test",
      "dummy_going_real",
      s"hdfs://localhost:8020${warehouseDir}/test.db/dummy_going_real",
      Format.Parquet,
      managed = true,
      Column("number", "int", partitioned = false, nullable = true) :: Nil)

    assert(obtained === expected)
  }

}
