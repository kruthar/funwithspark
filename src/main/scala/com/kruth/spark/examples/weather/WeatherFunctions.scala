package com.kruth.spark.examples.weather

/**
  * Created by kruthar on 11/16/15.
  */
object WeatherFunctions {
  val daysInMonths = List(31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)

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

  /**
    * Formats date string in format yyyyMMdd to format yyyy-MM-dd
    * @param dateString
    * @return formatted date string
    */
  def formatDateString(dateString: String): String = {
    return dateString.substring(0, 4) + "-" + dateString.substring(4, 6) + "-" + dateString.substring(6, 8)
  }

  /**
    * Is the given year a leap year?
    * @param year
    * @return true if given year is a leap year, false otherwise
    */
  def isLeapYear(year: Int): Boolean = {
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
      return true
    }
    return false
  }

  /**
    * Given a date calculate the day out of 365/366 of the year
    * @param year
    * @param month
    * @param day
    * @return the day of the year number
    */
  def getDayOfYear(year: Int, month: Int, day: Int): Int = {
    var doy = 0
    var counter = 0
    while (counter < month - 1) {
      doy += daysInMonths(counter)
      counter += 1
    }
    doy += day

    if (month > 2 && isLeapYear(year)) {
      doy += 1
    }

    return doy
  }
}
