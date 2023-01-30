package trg.data.http

import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.{Http, Service}
import com.twitter.util.{Await, Future}
import org.apache.spark.sql.{DataFrame, SparkSession}
import trg.data.kpi.KPIProvider._

case class HTTPServerHelper(df: DataFrame)(implicit spark: SparkSession) {

  private def getResultFromResponse(table: String): DataFrame = {
    table match {
      case "crimes" => df.limit(1000)
      case "differentCrimes" => getDifferentCrimes(df)
      case "differentOutcomes" => getDifferentOutcomes(df)
      case "crimesByLocation" => getCrimesByLocation(df)
      case "crimesByType" => getCrimesByType(df)
      case "crimesByOutcome" => getCrimesByOutcome(df)
      case "topCrimesPerLocation" => getTopCrimesPerLocation(df)
      case "outcomeStatusPerLocation" => getOutcomeStatusPerLocation(df)
      case "topCrimeOutcomesPerType" => getTopCrimeOutcomesPerType(df)
      case _ => spark.sql(
        """
          |SELECT
          |'crimes, differentCrimes, differentOutcomes, crimesByLocation, crimesByType,
          | crimesByOutcome, topCrimesPerLocation, outcomeStatusPerLocation, topCrimeOutcomesPerType'
          |AS available_endpoints
        """.stripMargin
      )
    }
  }

  def initHTTPServer(): Unit = {
    val service = new Service[Request, Response] {
      def apply(request: Request): Future[Response] = {
        val table = request.getParam("table")
        println(s"endpoint selected [/${table}]")

        val resultDF = getResultFromResponse(table)
        val response = Response()
        response.setContentString(resultDF.toJSON.collect.mkString("\n"))
        Future.value(response)
      }
    }

    val server = Http.serve(":9000", service)
    Await.ready(server)
  }

}
