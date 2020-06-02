package com.damavis.spark.pipeline.test

import com.damavis.spark.pipeline.{Pipeline, PipelineSource}
import org.apache.spark.sql.DataFrame

case class PipelineTester(pipeline: Pipeline, testPipeline: Pipeline) {

  def test(source: PipelineSource): DataFrame = {
    val zipped = pipeline.getStages zip testPipeline.getStages

    Pipeline(zipped.flatMap(x => List(x._1, x._2))).transform(source.get)
  }

}
