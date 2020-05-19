package com.damavis.spark.pipeline3

class PipelineStage(private val processor: Processor) {

  private var next: PipelineStage = NoStage
  private var dependsOn: Seq[PipelineStage] = Nil
  private var _toRun: Boolean = true

  protected def toRun: Boolean = _toRun

  protected def addDependency(stage: PipelineStage): Unit = {
    dependsOn = stage +: dependsOn
  }

  def compute(): Unit = {
    val canRun = !dependsOn.exists(_.toRun)

    if (canRun) {
      val newData = processor.compute()

      _toRun = false

      next.processor.setIn(newData)
      next.compute()
    }

  }

  def ->(stage: PipelineStage)(
      implicit definition: PipelineDefinition): PipelineStage = {
    if (this == stage)
      throw new RuntimeException("Loops not allowed in the pipeline")

    // TODO: Check for loops in the graph

    next = stage
    next.addDependency(this)

    stage
  }

  def ->(target: PipelineTarget)(
      implicit definition: PipelineDefinition): Pipeline = {
    next = target
    next.addDependency(this)

    new Pipeline(definition)
  }

}
