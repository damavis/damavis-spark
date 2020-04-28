package com.damavis.spark.pipeline.test

import com.damavis.spark.pipeline.PipelineTarget
import org.apache.spark.sql.DataFrame

trait PipelineTargetTest extends PipelineTarget {
  def test(data: DataFrame): Unit
  override def put(data: DataFrame): Unit = test(data)
}
