package org.mhr.nike

import scala.collection.mutable

import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._
import net.ceedubs.ficus.readers.namemappers.implicits.hyphenCase
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.concat_ws
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object Main {

  private val log = LoggerFactory.getLogger(getClass)

  def runSparkJob(sparkConf: SparkConf, appConf: Config): Unit = {
    val session = SparkSession.builder().config(sparkConf).getOrCreate()
    import session.implicits._

    val sales = session.read.option("header", "true").csv(appConf.salesPath) // TODO dynamic source options from config
    val dates = session.read.option("header", "true").csv(appConf.datesPath)
    val products = session.read.option("header", "true").csv(appConf.productsPath)
    val stores = session.read.option("header", "true").csv(appConf.storesPath)
    sales
      .join(dates, sales("dateId") === dates("dateId"))
      .join(products, sales("productId") === products("productId")) // TODO investigate ability broadcast here
      .join(stores.hint("broadcast"), sales("storeId") === stores("storeId"))
      .groupBy(
        products("gender"),
        stores("channel"),
        products("division"),
        products("category"),
        dates("datecalendaryear").cast(IntegerType).as("year"),
        dates("weeknumberofseason").cast(IntegerType).as("week")
      )
      .agg(
        sum(sales("netSales")).as("netSales"), // TODO support dynamic number of aggregations
        sum(sales("salesUnits")).as("salesUnits")
      )
      .withColumn(
        "uniqueId",
        concat_ws(sep = "_", $"year", $"gender", $"category", $"channel")
      )
      .as[AggregatedRow]
      .groupByKey(r => r.uniqueId -> r.week)
      .mapGroups {
        case (_, rows) =>
          rows.toStream match { // TODO support dynamic number of aggregations
            case h #:: tail =>
              val (netSales, salesUnits) =
                (h #:: tail).foldLeft((mutable.Map.empty[Int, Double], mutable.Map.empty[Int, Double])) {
                  case ((netSalesState, salesUnitsState), record) =>
                    netSalesState.put(record.week, record.netSales)
                    salesUnitsState.put(record.week, record.salesUnits)
                    (netSalesState, salesUnitsState)
                }
              val dataRows = List(
                ResultDataRows("netSales", setZeroValuesForEmptyWeeks(netSales)),
                ResultDataRows("salesUnits", setZeroValuesForEmptyWeeks(salesUnits))
              )
              Result(h.uniqueId, h.year, h.gender, h.channel, h.category, h.division, dataRows)
          }
      }
      .repartition(appConf.numOutputFiles) // TODO dynamic base on size fo row and number of records
      .write
      .mode(SaveMode.Overwrite) // TODO state management
      .json(appConf.outputPath)
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().registerKryoClasses(
      Array(Result.getClass, ResultDataRows.getClass, AggregatedRow.getClass)
    ).setMaster("local")
    val appConf = ConfigFactory.load().as[Config]("mhr.nike.test")
    log.info(s"Initial Config\n$appConf")
    runSparkJob(sparkConf, appConf)
  }

  private def setZeroValuesForEmptyWeeks(aggByWeek: mutable.Map[Int, Double]) =
    (1 to 52).map(week => week -> aggByWeek.getOrElse(week, 0d)).toMap

}
