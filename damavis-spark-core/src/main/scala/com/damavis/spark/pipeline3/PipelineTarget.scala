package com.damavis.spark.pipeline3

class PipelineTarget(processor: Processor) extends PipelineStage(processor) {

  override def compute(): Unit = processor.compute()

  override def ->(stage: PipelineStage)(
      implicit definition: PipelineDefinition): PipelineStage = ???

}
