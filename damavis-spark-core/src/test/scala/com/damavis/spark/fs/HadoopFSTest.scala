package com.damavis.spark.fs

import com.damavis.spark.entities.Author
import com.damavis.spark.utils.SparkTestSupport

class HadoopFSTest extends SparkTestSupport {

  import session.implicits._

  override def beforeAll(): Unit = {
    super.beforeAll()

    (Author("person1", 23, "ES") :: Author("person2", 346, "ES") :: Nil)
      .toDF()
      .write
      .format("parquet")
      .save("/persons")
  }

  "A Hadoop FS" should {
    "tell whether a path exists or not" in {
      val fs = HadoopFS()

      assert(fs.pathExists("/persons"))
      assert(!fs.pathExists("/persons2"))
    }
  }
}
