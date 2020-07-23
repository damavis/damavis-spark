package com.damavis.spark

import com.damavis.spark.resource.{ResourceReader, ResourceWriter}
import org.apache.spark.sql.DataFrame

import scala.language.implicitConversions

package object dataflow {

  object implicits {
    implicit def defaultSocketOfStage(stage: DataFlowStage): StageSocket =
      stage.left

    implicit def readerToSource(resource: ResourceReader): DataFlowSource = {
      val processor = new SourceProcessor {
        override def computeImpl(): DataFrame = resource.read()
      }

      new DataFlowSource(processor)
    }

    implicit def writerToTarget(resource: ResourceWriter): StageSocket = {
      val processor = new LinealProcessor {
        override def computeImpl(data: DataFrame): DataFrame = {
          resource.write(data)

          data
        }
      }

      val target = new DataFlowTarget(processor)

      target.left
    }
  }

}
