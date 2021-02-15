package com.damavis.spark.fs

import com.damavis.spark.utils.SparkTestBase
import com.damavis.spark.testdata._

class HadoopFSTest extends SparkTestBase {

  override def beforeAll(): Unit = {
    super.beforeAll()

    dfFromAuthors(bradbury).write
      .format("parquet")
      .save("/persons")

    dfFromBooks(martian).write
      .format("parquet")
      .save("/persons/books/")
  }

  "A Hadoop FS" should {
    "tell whether a path exists or not" in {
      val fs = HadoopFS()

      assert(fs.pathExists("/persons"))
      assert(!fs.pathExists("/persons2"))
    }

    "list directories properly" in {
      val fs = HadoopFS()

      assert(fs.listSubdirectories("/persons") === ("books" :: Nil))
      assert(fs.listSubdirectories("/persons/books").isEmpty)
    }

    "throw exception when asked for listing a file" in {
      val fs = HadoopFS()

      assertThrows[IllegalArgumentException] {
        fs.listSubdirectories("/persons/_SUCCESS")
      }
    }
  }
}
