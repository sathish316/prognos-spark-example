package jobs

import com.prognos.Series
import com.prognos.forecast.HoltLinear
import org.apache.spark.{SparkConf, SparkContext}

case class PassengerData(year:Int, country:String, value:Double)

object AirPassengerForecastingJob {

  def main(args: Array[String]): Unit = {
    val inputFile = args(0)
    val outputFile = args(1)
    val conf = new SparkConf().setMaster("local[2]").setAppName("air-passenger-forecasting")
    val sc = new SparkContext(conf)
    sc.textFile(inputFile)
      .map(parseLine)
      .groupBy(_.country)
      .mapValues(observations)
      .mapValues(forecast)
      .saveAsTextFile(outputFile)
  }

  def parseLine(line: String): PassengerData = {
    val Array(year, country, passengers) = line.split(",")
    PassengerData(year.toInt, country, passengers.toDouble)
  }

  def observations(passengers: Iterable[PassengerData]): List[Double] = {
    passengers.toList.sortBy(_.year).map(_.value)
  }

  def forecast(observations:List[Double]):Double = {
    val series = new Series(observations.toArray)
    val algo = new HoltLinear
    val (alpha, beta, algoType, horizon) = (0.8, 0.2, "simple", 1)
    val forecasts = algo.calculate(series, alpha, beta, algoType, horizon)
    forecasts(0)
  }
}
