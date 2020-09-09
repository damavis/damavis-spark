package com.damavis.spark.utils

import com.holdenkarau.spark.testing.{
  DataFrameSuiteBase,
  HDFSClusterLike,
  SharedSparkContext,
  SparkContextProvider
}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.WordSpec

class SparkTestBase
    extends WordSpec
    with DataFrameSuiteBase
    with SparkTestSupport
    with SparkContextProvider
    with HDFSClusterLike
    with SharedSparkContext {

  var hdfsUri: String = _
  val name: String = this.getClass.getSimpleName
  val warehouseConf: String = s"/tmp/sparktest-$name-warehouse"

  lazy val root: String = s"${HDFSCluster.uri}/$name"
  lazy implicit val session: SparkSession = spark

  System.setSecurityManager(null) // Required hack

  override def conf: SparkConf = {
    new SparkConf()
      .setAppName(name)
      .setMaster("local[*]")
      .set("spark.sql.catalogImplementation", "hive")
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog",
           "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .set("spark.hadoop.fs.default.name", HDFSCluster.uri)
      .set("spark.sql.warehouse.dir", warehouseConf) // Ignored by Holden Karau
  }

}
