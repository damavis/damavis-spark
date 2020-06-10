package com.damavis.spark.database

import com.damavis.spark.resource.Format.Format

case class TableOptions(path: String, format: Format, managed: Boolean)
