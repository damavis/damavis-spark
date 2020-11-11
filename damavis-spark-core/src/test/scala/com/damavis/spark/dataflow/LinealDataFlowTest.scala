package com.damavis.spark.dataflow

import com.damavis.spark.dataflow.entities._
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.DataFrame
import org.scalatest.WordSpec

class LinealDataFlowTest extends WordSpec with DataFrameSuiteBase {

  import spark.implicits._
  import utils.implicits._

  "A lineal pipeline" should {
    "deliver dataFrames across stages" in {

      val source: DataFlowSource = (Person("Frollo", 50) :: Nil).toDF

      val targetProcessor = new LinealProcessor {
        override def computeImpl(data: DataFrame): DataFrame = {
          val expected = (Person("Frollo", 50) :: Nil).toDF

          assertDataFrameEquals(expected, data)

          data
        }
      }

      val target = new DataFlowTarget(targetProcessor)

      val pipeline = DataFlowBuilder.create { implicit definition =>
        import implicits._

        source -> target

      }

      pipeline.run()

    }
  }
}
