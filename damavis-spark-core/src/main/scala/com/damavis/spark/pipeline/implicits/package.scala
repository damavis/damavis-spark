package com.damavis.spark.pipeline

import scala.language.implicitConversions
import com.damavis.spark.resource.{ResourceReader, ResourceWriter}
import org.apache.spark.sql.DataFrame

package object implicits {

  implicit def readerToSource(resource: ResourceReader): PipelineSource =
    new PipelineSource {
      override def get: DataFrame = resource.read()
    }

  implicit def writerToTarget(resource: ResourceWriter): PipelineTarget =
    new PipelineTarget {
      override def put(data: DataFrame): Unit = resource.write(data)
    }

}
