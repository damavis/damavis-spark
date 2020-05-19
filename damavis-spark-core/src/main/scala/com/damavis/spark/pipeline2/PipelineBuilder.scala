package com.damavis.spark.pipeline2

object PipelineBuilder {

  def create(f: PipelineDefinition => Pipeline): Pipeline = {
    this.synchronized {
      val definition = new PipelineDefinition

      f(definition)

      //TODO: check pipeline is correct (no internal loops, no null pointers)
      new Pipeline(definition)
    }
  }

}
