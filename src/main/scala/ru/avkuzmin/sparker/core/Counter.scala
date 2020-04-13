package ru.avkuzmin.sparker.core

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


object Counter extends App{

  System.setProperty("hadoop.home.dir", "hadoop")
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("org").setLevel(Level.OFF)

  val sc = new SparkContext("local[*]", "Spark Counter", "temp")

  val fileName = "src/main/resources/1984.txt"
  val data: RDD[String] = sc.textFile(fileName)
  val counts = data.
    flatMap(line => line
      .replaceAll("[^а-яА-Яё]"," ")
      .split(" "))
    .map(word => (word.toUpperCase, 1)).reduceByKey(_+_,1)
    .map(item => item.swap)
    .sortByKey(false,1)

  println(counts.top(100).mkString("\n"))

}
