package org.mhr.nike.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.mhr.nike.Config
import org.mhr.nike.Main
import org.scalatest.FunSuite

// TODO On yarn Test using <code>org.apache.spark.launcher.SparkLauncher</code>
class LocalSparkTest extends FunSuite {

  // TODO more cases
  test("Happy path") {
    val sparkConf = new SparkConf().setMaster("local")
    val outputPath = "test/consumption/"
    Main.runSparkJob(
      sparkConf,
      Config("sales.csv", "calendar.csv", "product.csv", "store.csv", outputPath, 2)
    )
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val expectedRowCount = 3
    assert(spark.read.json(outputPath).count() == expectedRowCount)
  }

}
