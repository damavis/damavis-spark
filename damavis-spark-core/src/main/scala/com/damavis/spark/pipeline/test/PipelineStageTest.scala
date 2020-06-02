package com.damavis.spark.pipeline.test

import com.damavis.spark.pipeline.PipelineStage
import org.apache.spark.sql.DataFrame

trait PipelineStageTest extends PipelineStage {

  override def transform(data: DataFrame): DataFrame = {
    try {
      test(data)
      data
    } catch {
      case e: Exception =>
        throw new PipelineException(
          s"Assertion error at ${this.getClass}:\n" + e.getMessage)
    }
  }

  def test(data: DataFrame): Unit

}
