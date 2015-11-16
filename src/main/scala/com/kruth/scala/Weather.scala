package com.kruth.scala

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex

/**
  * Created by kruthar on 11/16/15.
  */
object WeatherFunctions {
  /**
    * Converts a Celsius temperature to Fharenheit
    * @param temp
    * @return temperature in Fharenheit
    */
  def CelsiusToFahrenheit(temp: Double): Double = {
    return temp * 9 / 5 + 32
  }

  /**
    * Pad the front of a given string with 0
    * @param day
    * @param len
    * @return padded String
    */
  def padWithZero(day: String, len: Int): String = {
    if (day.length() < len) {
      var result = day
      while (result.length < len) {
        result = "0" + result
      }
      return result
    }
    return day
  }

  def formatDateString(dateString: String): String = {
    return dateString.substring(0, 4) + "-" + dateString.substring(4, 6) + "-" + dateString.substring(6, 8)
  }
}

object Weather {
  def main(args: Array[String]): Unit = {
    val monthlyWeatherSplitRegex = new Regex("^(.{11})(.{4})(.{2})(.{4})(.{8})(.{8})(.{8})(.{8})(.{8})" +
      "(.{8})(.{8})(.{8})(.{8})(.{8})(.{8})(.{8})(.{8})(.{8})(.{8})(.{8})(.{8})(.{8})" +
      "(.{8})(.{8})(.{8})(.{8})(.{8})(.{8})(.{8})(.{8})(.{8})(.{8})(.{8})(.{8})(.{8})$")
    val dailyValueSplitRegex = new Regex("^(.{5})(.)(.)(.)$")
    val dateFormat = new SimpleDateFormat("yyyyMMdd")

    val conf = new SparkConf()
      .setAppName("Weather")
    val sc = new SparkContext(conf)

    /**
      * Grab a local weather text file with lines of the form of:
      * AE000041196194403TMAX-9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999   \
      * -9999   -9999   -9999   -9999   -9999   -9999   -9999   -9999     380  I  346  I  319  I  302  I  296  I  \
      * 308  I  291  I  296  I  296  I  335  I  396  I  313  I
       */
    val rawWeather = sc.textFile("AE000041196.dly")

    /**
      * Used a fixed length Regex pattern to break each line into a list containing meta and daily values. splitWeather
      * lines will be in the form of:
      *
      * List([station], [year], [month], [type], [day1], ... , [day31])
      */
    val splitWeather: RDD[List[String]] = rawWeather
      .map(line => monthlyWeatherSplitRegex.findFirstMatchIn(line).get.subgroups)

    /**
      * Each line contains values for every day of a given month, flatmap each line so that each new line represents a
      * reading of a single day. dailyWeather lines will be in the form of:
      *
      * (List([stations], [year], [month], [type]), ([day], List([value], [MFlag], [QFlag], [SFlag])))
      *
      * Cache that RDD as we will be using it to find multiple things
      */
    val dailyWeather = splitWeather
      .flatMap(month => for {
        day <- month.slice(4, month.size).zipWithIndex
      } yield (month.slice(0, 4), (WeatherFunctions.padWithZero((day._2 + 1).toString, 2), dailyValueSplitRegex.findFirstMatchIn(day._1).get.subgroups)))
      .cache()

    /**
      * Find the count of invalid readings
      */
    val invalidReadings = dailyWeather
      .filter(daily => !daily._2._2(0).startsWith("-9999"))
      .count()
    println("Invalid Readings: " + invalidReadings)

    /**
      * Filter out all of the invalid readings so we have a clean set.
      * Cache this RDD for multiple computation paths.
      */
    val validDailyWeather = dailyWeather
      .filter(daily => !daily._2._2(0).startsWith("-9999"))
      .cache()

    /**
      * Remove this from teh cache as it is no longer needed
      */
    dailyWeather.unpersist()

    /**
      * Print the date range of this data
      */
    var boundaryDates = validDailyWeather
      .map(daily => (daily._1(1) + daily._1(2) + daily._2._1, daily._1(1) + daily._1(2) + daily._2._1))
      .fold(("99999999", "00000000"))((acc, daily) => {
        var result = acc
        if (daily._1 < acc._1)
          result = result.copy(_1 = daily._1)
        if (daily._2 > acc._2)
          result = result.copy(_2 = daily._2)
        result
      })
    boundaryDates = boundaryDates.copy(
      _1 = WeatherFunctions.formatDateString(boundaryDates._1),
      _2 = WeatherFunctions.formatDateString(boundaryDates._2)
    )

    /**
      * Find the hottest TMAX reading.
      */
    val hottestTemperature = validDailyWeather
      .filter(daily => daily._1(3) == "TMAX")
      .reduce((a, b) => if (a._2._2(0).trim.toInt > b._2._2(0).trim.toInt) a else b)

    /**
      * Find the coldest TMIN reading.
      */
    val coldestTemperature = validDailyWeather
      .filter(daily => daily._1(3) == "TMIN")
      .reduce((a, b) => if (a._2._2(0).trim.toInt < b._2._2(0).trim.toInt) a else b)

    println("Date Range: " + boundaryDates)
    println("Hottest Temperature: " + hottestTemperature +
      " Fharenheit: " + WeatherFunctions.CelsiusToFahrenheit(hottestTemperature._2._2(0).toDouble / 10))
    println("Coldest Temperature: " + hottestTemperature +
      " Fharenheit: " + WeatherFunctions.CelsiusToFahrenheit(coldestTemperature._2._2(0).toDouble / 10))
  }
}
