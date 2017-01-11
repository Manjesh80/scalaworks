/**
  *
  * Author: mg153v (Manjesh Gowda). Creation Date: 1/11/2017.
  */

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.log4j.{Level, Logger}

object NumberCalculator {
  def main(args: Array[String]): Unit = {

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.OFF)

    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("CustomReceiver")

    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.sparkContext.setLogLevel("ERROR")
    val lines = ssc.receiverStream(new NumberCalculator)

    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

class NumberCalculator()
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  def onStart() {
    new Thread("Receiver") {
      override def run() {
        receive()
      }
    }.start()
  }

  def onStop() {
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {
    while (!isStopped()) {
      for (i <- 1 to 100) {
        //store(scala.util.Random.nextInt(1000));
        store("Jai Ganesh")
      }
      Thread.sleep(1)
    }
  }
}
