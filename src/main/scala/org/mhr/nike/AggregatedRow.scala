package org.mhr.nike

case class AggregatedRow(
    uniqueId: String,
    gender: String,
    channel: String,
    division: String,
    category: String,
    year: Int,
    week: Int,
    netSales: Double, // TODO manually specify schema fro csv and use Decimal
    salesUnits: Double)
