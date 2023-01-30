package trg.data.apps

import org.apache.spark.sql.SparkSession

import scala.reflect.io.File

object DownloadDataAsCSV {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("DownloadDataAsCSV")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val parquetDataPath = "./data_store/crimes"

    if (File(parquetDataPath).exists) {
      println("Saving crimes table to csv file...")
      spark.table(s"parquet.`${parquetDataPath}`")
        .coalesce(1)
        .write
        .format("csv")
        .mode("overwrite")
        .option("header", "true")
        .option("delimiter", ",")
        .save("./src/main/output/crimes_data")
    } else {
      println("Silver table has not been created yet. Please run ETLPipeline first")
    }
  }

}
