package com.damavis.spark.dataflow

object DataFlowBuilder {

  def create(f: DataFlowDefinition => Any): DataFlow = {
    this.synchronized {
      val definition = new DataFlowDefinition

      f(definition)

      //TODO: check pipeline is correct (check DataFlowBuilderTest for all the validations to implement here)
      new DataFlow(definition)
    }
  }

}
