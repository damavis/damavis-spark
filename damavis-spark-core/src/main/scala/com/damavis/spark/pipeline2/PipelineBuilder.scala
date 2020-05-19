package com.damavis.spark.pipeline2

object PipelineBuilder {

  def create(f: PipelineDefinition => Any): Pipeline = {
    this.synchronized {
      val definition = new PipelineDefinition

      f(definition)

      //TODO: check pipeline is correct (check PipelineBuilderTest for all the validations to implement here)
      new Pipeline(definition)
    }
  }

}
