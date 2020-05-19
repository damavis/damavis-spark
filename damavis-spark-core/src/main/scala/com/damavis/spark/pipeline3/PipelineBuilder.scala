package com.damavis.spark.pipeline3

object PipelineBuilder {

  def create(f: PipelineDefinition => Pipeline): Pipeline = {
    this.synchronized {
      val definition = new PipelineDefinition
      f(definition)
    }
  }

}
