package com.damavis.spark.pipeline2

case class PipelineDefinition(source: PipelineSource,
                              stages: Seq[PipelineStage],
                              target: PipelineTarget = null) {
  def ->(next: PipelineStage): PipelineDefinition = {
    copy(stages = stages :+ next)
  }

  def ->(target: PipelineTarget): Pipeline = {
    val finalDefinition = copy(target = target)

    new Pipeline(finalDefinition)
  }

}
