package com.damavis.spark.pipeline2
import org.apache.spark.sql.DataFrame

trait PipelineTarget {

  def put(data: DataFrame): Unit

}
