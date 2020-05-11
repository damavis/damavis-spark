package com.damavis.spark.database

import com.damavis.spark.resource.datasource.Format.Format

case class TableOptions(path: String, format: Format, managed: Boolean)
