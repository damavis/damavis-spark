package com.damavis.spark.entities

import java.time.LocalDate

case class Author(name: String,
                  deceaseAge: Int,
                  birthDate: LocalDate,
                  nationality: String)
