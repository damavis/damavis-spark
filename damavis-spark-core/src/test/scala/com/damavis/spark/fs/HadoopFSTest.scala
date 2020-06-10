package com.damavis.spark.fs

import com.damavis.spark.utils.SparkTestSupport
import com.damavis.spark._

class HadoopFSTest extends SparkTestSupport {

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
