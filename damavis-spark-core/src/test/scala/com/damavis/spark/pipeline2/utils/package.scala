package com.damavis.spark.pipeline2

import org.apache.spark.sql.DataFrame

import scala.language.implicitConversions

package object utils {

  object implicits {

    implicit def sourceFromDf(df: DataFrame): PipelineSource = {
      val processor = new SourceProcessor {
        override def computeImpl(): DataFrame = df
      }

      new PipelineSource(processor)
    }
  }

}
