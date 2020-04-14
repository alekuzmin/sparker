package ru.avkuzmin.sparker.core

import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint


object Logreg extends App {

  val fileName = "src/main/resources/diabet.csv"

  val sc = new SparkContext("local[2]", "Simple Log Reg")
  val data = sc.textFile(fileName)
  val parsedData = data.map(line => {
    val parts = line.split(",")
    LabeledPoint(parts(8).toDouble, Vectors.dense(parts.slice(0,8).map(_.toDouble)))
  })

  val splitsData = parsedData.randomSplit(Array(0.8,0.2))
  val trainingData = splitsData(0)
  val testingData = splitsData(1)

  val model = new LogisticRegressionWithLBFGS()
    .setNumClasses(2)
    .run(trainingData)

 val predictionsAndLabels = testingData.map(x => {
    val prediction = model.predict(x.features)
   (prediction, x.label)
  })

  val metrics = new MulticlassMetrics(predictionsAndLabels)
  val accuracy = metrics.accuracy

  println(Console.GREEN + s"Веса: ${model.weights}" + Console.WHITE)
  println(Console.GREEN + s"Точность: ${accuracy}" + Console.WHITE)

}
