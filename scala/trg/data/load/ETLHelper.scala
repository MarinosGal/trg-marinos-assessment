package trg.data.load

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object ETLHelper {

  /**
   * Loads a csv file that has a header
   *
   * @param path the file path
   * @param spark sparkSession
   * @return a Spark DataFrame with the loaded data
   */
  private def loadCSV(path: String)(implicit spark: SparkSession): DataFrame = {
    spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)
  }

  /**
   * Based on loadCSV function, loads all the street crimes related files
   *
   * @param spark sparkSession
   * @return a Spark DataFrame with the street crimes data
   */
  def fetchStreetCrimes(implicit spark: SparkSession): DataFrame = {
    loadCSV("./src/main/resources/street-crimes-uk/*/*street.csv")
  }

  /**
   * Based on loadCSV function, loads all the crimes outcomes related files
   *
   * @param spark sparkSession
   * @return a Spark DataFrame with the crimes outcomes data
   */
  def fetchCrimeOutcomes(implicit spark: SparkSession): DataFrame = {
    loadCSV("./src/main/resources/street-crimes-uk/*/*outcomes.csv")
  }

  /**
   * Creates the silver table for crimes
   *
   * @param streetDF   the input street crimes data
   * @param outcomesDF the input outcomes data
   * @param spark      sparkSession
   * @return a Spark DataFrame with the requested crimes data
   */
  def constructCrimesTable(streetDF: DataFrame, outcomesDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    // regex pattern to extract the location names from csv files
    val extractFilenamePattern = "[0-9]{4}-[0-9]{2}-(.*?)-street\\.csv"

    streetDF
      .withColumn("districtName",
        regexp_replace(
          regexp_extract(
            element_at(
              split(input_file_name, "/"), // split file path by /
              -1), // take the last element of the array
            extractFilenamePattern, 1) // extract with regex the location name
          , "-", " ") // replace any - with space
      )
      .as("s")
      .join(outcomesDF.as("o"), $"s.Crime ID" === $"o.Crime ID", "left")
      .withColumn("lastOutcome", coalesce($"o.Outcome type", $"s.Last outcome category"))
      .select("s.Crime ID", "s.districtName", "s.Latitude", "s.Longitude", "s.Crime type", "lastOutcome")
      .withColumnRenamed("Crime ID", "crimeID")
      .withColumnRenamed("Crime type", "crimeType")
      .withColumnRenamed("Latitude", "latitude")
      .withColumnRenamed("Longitude", "longitude")
      .withColumn("latitude", $"latitude".cast("decimal(8,6)"))
      .withColumn("longitude", $"longitude".cast("decimal(9,6)"))
  }

  /**
   * Saves crimes data as managed table on parquet files
   * @param df the input DataFrame with the requested crimes data
   * @param path the path to data
   * @param spark sparkSession
   */
  def saveCrimesTable(df: DataFrame, path: String)(implicit spark: SparkSession): Unit = {
    df.dropDuplicates()
      .coalesce(1) // less than 200MB on disk
      .write
      .format("parquet")
      .mode("overwrite")
      .save(path)
  }

}
