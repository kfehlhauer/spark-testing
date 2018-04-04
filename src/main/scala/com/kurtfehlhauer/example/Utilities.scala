package com.kurtfehlhauer.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object Utilities {
  def wordCount(rdd: RDD[String], spark: SparkSession): RDD[(String, Int)] = {
    rdd.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .sortByKey()
  }

  def numberBetweenZeroandTen(s: Int): Int = {
    val r = scala.util.Random
    r.setSeed(s)
    r.nextInt(10)
  }
}

