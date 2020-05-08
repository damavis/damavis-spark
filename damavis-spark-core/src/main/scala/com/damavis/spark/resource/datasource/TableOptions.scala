package com.damavis.spark.resource.datasource

import com.damavis.spark.resource.datasource.Format._

case class TableOptions(name: String, path: String, format: Format)
