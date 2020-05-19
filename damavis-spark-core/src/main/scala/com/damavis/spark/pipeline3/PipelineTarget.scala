package com.damavis.spark.pipeline3

class PipelineTarget(processor: Processor) extends PipelineStage(processor) {

  override def compute(): Unit = processor.compute(sockets)

  override def ->(stage: StageSocket)(
      implicit definition: PipelineDefinition): PipelineStage = ???

}