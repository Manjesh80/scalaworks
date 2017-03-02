/**
  *
  * Author: mg153v (Manjesh Gowda). Creation Date: 3/1/2017.
  */

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors


object RegressionDemo {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("Basic linear regerssion algorithm demo")
      .config("spark.master", "local")
      .getOrCreate();

    import spark.implicits._

    val data = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .format("csv")
      .load("USA-Housing.csv");

    val df = data.select(data("Price").as("label"), $"Avg Area Income", $"Avg Area House Age", $"Avg Area Number of Rooms", $"Area Population")
    val assembler = new VectorAssembler().setInputCols(Array("Avg Area Income", "Avg Area House Age", "Avg Area Number of Rooms", "Area Population")).setOutputCol("features")
    val output = assembler.transform(df).select($"label", $"features")

    val lr = new LinearRegression()
    val lrModel = lr.fit(output)

    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
    val trainingSummary = lrModel.summary

    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")

    trainingSummary.residuals.show()

    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"MSE: ${trainingSummary.meanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")
  }
}
