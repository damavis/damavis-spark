package com.damavis.spark

import org.scalatest.{FlatSpec, Matchers}

class SparkAppTest extends FlatSpec with Matchers with SparkApp {

  override val name: String = "SparkAppTest"

  "An SparkApp" should
    "run successfully" in {
    import session.implicits._
    val df = spark.sparkContext.parallelize(List(1, 2, 3)).toDF("number")
    assert(df.count() === 3)
    spark.stop()
  }

}
