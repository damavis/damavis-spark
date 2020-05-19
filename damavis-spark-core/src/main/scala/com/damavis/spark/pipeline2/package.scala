package com.damavis.spark

import com.damavis.spark.resource.{ResourceReader, ResourceWriter}
import org.apache.spark.sql.DataFrame

import scala.language.implicitConversions

package object pipeline2 {

  object implicits {
    implicit def defaultSocketOfStage(stage: PipelineStage): StageSocket =
      stage.left

    implicit def readerToSource(resource: ResourceReader): PipelineSource = {
      val processor = new SourceProcessor {
        override def computeImpl(): DataFrame = resource.read()
      }

      new PipelineSource(processor)
    }

    implicit def writerToTarget(resource: ResourceWriter): StageSocket = {
      val processor = new LinealProcessor {
        override def computeImpl(data: DataFrame): DataFrame = {
          resource.write(data)

          data
        }
      }

      val target = new PipelineTarget(processor)

      target.left
    }
  }

}
