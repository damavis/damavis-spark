package com.damavis.spark.pipeline3

import org.apache.spark.sql.DataFrame

object NoProcessor extends Processor {
  override def compute(): DataFrame = in
}

object NoStage extends PipelineStage(NoProcessor) {}
