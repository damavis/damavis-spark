package com.damavis.spark.dataflow

import org.apache.spark.sql.DataFrame

import scala.language.implicitConversions

package object utils {

  object implicits {

    implicit def sourceFromDf(df: DataFrame): DataFlowSource = {
      val processor = new SourceProcessor {
        override def computeImpl(): DataFrame = df
      }

      new DataFlowSource(processor)
    }
  }

}
