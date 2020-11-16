package org.mhr.nike

case class Result(
    uniqueId: String,
    year: Int,
    gender: String,
    division: String,
    channel: String,
    category: String,
    dataRows: List[ResultDataRows]
)
