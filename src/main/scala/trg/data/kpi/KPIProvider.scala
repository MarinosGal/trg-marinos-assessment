package trg.data.kpi

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object KPIProvider {

  /**
   * Get the distinct crimes
   * @param df the crimes dataframe
   * @return a dataframe with the requested data
   */
  def getDifferentCrimes(df: DataFrame): DataFrame = {
    df.select("crimeType")
      .distinct()
      .withColumnRenamed("crimeType", "differentCrimes")
  }

  /**
   * Get the distinct crime outcomes
   * @param df the crimes dataframe
   * @return a dataframe with the requested data
   */
  def getDifferentOutcomes(df: DataFrame): DataFrame = {
    df.select("lastOutcome")
      .distinct()
      .withColumnRenamed("lastOutcome", "differentOutcomes")
  }

  /**
   * Get the crimes per location
   * @param df the crimes dataframe
   * @return a dataframe with the requested data
   */
  def getCrimesByLocation(df: DataFrame): DataFrame = {
    df.groupBy("districtName")
      .agg(count("crimeId").as("totalCrimes"))
      .orderBy(desc("totalCrimes"))
      .select("districtName", "totalCrimes")
  }

  /**
   * Get the crimes per type
   * @param df the crimes dataframe
   * @return a dataframe with the requested data
   */
  def getCrimesByType(df: DataFrame): DataFrame = {
    df.groupBy("crimeType")
      .agg(count("crimeId").as("totalCrimes"))
      .orderBy(desc("totalCrimes"))
      .select("crimeType", "totalCrimes")
  }

  /**
   * Get the crimes per crime outcome
   * @param df the crimes dataframe
   * @return a dataframe with the requested data
   */
  def getCrimesByOutcome(df: DataFrame): DataFrame = {
    df.groupBy("lastOutcome")
      .agg(count("crimeId").as("totalCrimes"))
      .orderBy(desc("totalCrimes"))
      .withColumnRenamed("lastOutcome", "outcome")
      .select("outcome", "totalCrimes")
  }

  /**
   * Get the top 3 crime types per location
   * @param df the crimes dataframe
   * @return a dataframe with the requested data
   */
  def getTopCrimesPerLocation(df: DataFrame): DataFrame = {
    df.groupBy("districtName", "crimeType")
      .agg(count("crimeType").as("count_crime_types"))
      .withColumn("row_number", row_number().over(Window.partitionBy("districtName").orderBy(desc("count_crime_types"))))
      .where("row_number <= 3")
      .groupBy("districtName")
      .agg(collect_list("crimeType").as("top_crime_types"))
  }

  /**
   * Get the outcome status per location
   * UNRESOLVED: Unable to prosecute suspect
   * PENDING: Under investigation
   * RESOLVED: the rest of the statuses
   * @param df the crimes dataframe
   * @return a dataframe with the requested data
   */
  def getOutcomeStatusPerLocation(df: DataFrame): DataFrame = {
    df.withColumn("lastOutcomeStatus", when(col("lastOutcome") === "Unable to prosecute suspect", "UNRESOLVED")
      .when(col("lastOutcome") === "Under investigation", "PENDING")
      .otherwise("RESOLVED"))
      .groupBy("districtName", "lastOutcomeStatus")
      .agg(count("lastOutcomeStatus").as("countLastOutcomeStatus"))
      .withColumn("row_number", row_number().over(Window.partitionBy("districtName").orderBy(desc("countLastOutcomeStatus"))))
      .drop("row_number")
      .orderBy("districtName")
  }

  /**
   * Get the top crime outcome per crime type
   * @param df the crimes dataframe
   * @return a dataframe with the requested data
   */
  def getTopCrimeOutcomesPerType(df: DataFrame): DataFrame = {
    df.groupBy("crimeType", "lastOutcome")
      .agg(count("lastOutcome").as("countLastOutcomes"))
      .withColumn("row_number", row_number().over(Window.partitionBy("crimeType").orderBy(desc("countLastOutcomes"))))
      .where("row_number = 1")
      .groupBy("crimeType")
      .agg(collect_list("lastOutcome")(0).as("topOutcome"))
      .drop("row_number")
  }

}
