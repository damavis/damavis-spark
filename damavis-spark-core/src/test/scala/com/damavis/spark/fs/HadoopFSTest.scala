package com.damavis.spark.fs

import com.damavis.spark.utils.{SparkTestBase}
import com.damavis.spark.testdata._

class HadoopFSTest extends SparkTestBase {

  override def beforeAll(): Unit = {
    super.beforeAll()

    dfFromAuthors(bradbury).write
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
