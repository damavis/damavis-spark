package com.damavis.spark.dataflow

class PipelineTarget(processor: Processor) extends PipelineStage(processor) {

  override def compute(): Unit = processor.compute(sockets)

  override def ->(stage: StageSocket)(
      implicit definition: PipelineDefinition): PipelineStage = ???

}
