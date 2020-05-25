package com.damavis.spark.fs

import com.damavis.spark.entities.Person
import com.damavis.spark.utils.SparkTestSupport

class HadoopFSTest extends SparkTestSupport {

  import session.implicits._

  override def beforeAll(): Unit = {
    super.beforeAll()

    (Person("person1", 23) :: Person("person2", 346) :: Nil)
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
