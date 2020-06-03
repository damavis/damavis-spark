package com.damavis.spark.resource.file

import com.damavis.spark.resource.Format.Format

case class FileWriterParameters(format: Format,
                                path: String,
                                mode: String = "overwrite",
                                columnNames: Seq[String] = Nil)
