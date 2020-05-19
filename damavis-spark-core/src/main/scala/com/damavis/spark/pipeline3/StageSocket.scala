package com.damavis.spark.pipeline3

import org.apache.spark.sql.DataFrame

class StageSocket(val stage: PipelineStage) {
  private var data: DataFrame = _

  def get: DataFrame = data
  def set(data: DataFrame): Unit = this.data = data

}
