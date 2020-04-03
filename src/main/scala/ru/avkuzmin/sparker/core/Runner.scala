package ru.avkuzmin.sparker.core

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Runner extends App {

  System.setProperty("hadoop.home.dir", "hadoop/bin/winutils.exe")
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("org").setLevel(Level.OFF)

  val fileName = "src/main/resources/1984.txt"
  val config = new SparkConf()
  config.setAppName("my spark core")
  config.setMaster("local[*]")

  val sc = new SparkContext(config)
  val data: RDD[String] = sc.textFile(fileName)
  val words = data
    .flatMap(w => w
      .replace(","," ")
      .replace("."," ")
      .split(" "))
    .persist(StorageLevel.MEMORY_ONLY)

  def findStartWith(words: RDD[String], word: String) = {
    println(word + ": " + words.filter(w => w.startsWith(word)).count())
  }

  def findCompareToIgnoreCase(words: RDD[String], word: String) = {
    println(word + ": " + words.filter(w => w.compareToIgnoreCase(word) == 0).count())
  }

  findStartWith(words, "О’Брайен")
  findStartWith(words, "Джулия")
  findStartWith(words, "Смит")

  findCompareToIgnoreCase(words, "министерство")
  findCompareToIgnoreCase(words, "война")
  findCompareToIgnoreCase(words, "время")

  //For jobs monitoring on http://localhost:4040/jobs
  //Thread.sleep(9999999999999L)
}
