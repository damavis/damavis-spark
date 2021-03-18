package com.damavis.spark.pipeline.predefined

import com.damavis.spark.pipeline.PipelineStage
import org.apache.spark.sql.DataFrame

class CacheStage extends PipelineStage {

  override def transform(data: DataFrame): DataFrame = data.cache()
}
