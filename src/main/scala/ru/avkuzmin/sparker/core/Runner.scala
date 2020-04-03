package ru.avkuzmin.sparker.core

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Runner extends App {

  System.setProperty("hadoop.home.dir", "F:\\work\\!ML\\spark\\hadoop_winutils")
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val fileName = "C:\\Users\\Alex\\IdeaProjects\\sparker\\src\\main\\resources\\1984.txt"
  //String word = "боль"
  val config = new SparkConf()
  config.setAppName("my spark core")
  config.setMaster("local[*]")

  val sc = new SparkContext(config)
  val data: RDD[String] = sc.textFile(fileName)
  val words = data
    .flatMap(w => w
      //.replaceAll(","," ")
      //.replaceAll("."," ")
      .split(" "))
    .persist(StorageLevel.MEMORY_ONLY)

  var word: String = "Язык"

  val countW = words.filter(w => w.startsWith("язык"))
  println(word + ": " + countW.count())

}
