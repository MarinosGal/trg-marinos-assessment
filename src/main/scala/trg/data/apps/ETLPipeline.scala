package trg.data.apps

import org.apache.spark.sql.SparkSession
import trg.data.http.HTTPServerHelper
import trg.data.load.ETLHelper._

import scala.reflect.io.File

object ETLPipeline {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("ETLPipeline")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val parquetDataPath = "./data_store/crimes"

    // if the silver does not exists; start etl
    if (!File(parquetDataPath).exists) {

      println("Loading files for street crimes...")
      val streetDF = fetchStreetCrimes(spark)

      println("Loading files for crime outcomes...")
      val outcomesDF = fetchCrimeOutcomes(spark)

      println("Constructing silver table...")
      val crimesDF = constructCrimesTable(streetDF, outcomesDF)(spark)

      println("Saving silver table...")
      saveCrimesTable(crimesDF, parquetDataPath)(spark)
    }

    val httpServer = HTTPServerHelper(spark.table(s"parquet.`$parquetDataPath`").cache)(spark)
    println("HTTP Server started!")
    httpServer.initHTTPServer()
  }
}
