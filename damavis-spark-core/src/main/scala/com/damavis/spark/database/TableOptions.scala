package com.damavis.spark.database

import com.damavis.spark.resource.datasource.enums.Format._

case class TableOptions(path: String, format: Format, managed: Boolean)
