import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

object EvenOddCalculator {
  def main(args: Array[String]): Unit = {

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.OFF)

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("CustomReceiver")
    val ssc = new StreamingContext(sparkConf, Seconds(3)) // Batch Interval of 3 seconds
    ssc.sparkContext.setLogLevel("ERROR")
    val numbers = ssc.receiverStream(new EvenOddCalculator)
    ssc.checkpoint("./chkpoint")
    val mappedNumbers = numbers.map(x => (if (x % 2 == 0) "EVEN" else "ODD", 1))
      .reduceByKeyAndWindow((x, y) => x + y, (x, y) => x - y, Seconds(15), Seconds(6))
    mappedNumbers.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

class EvenOddCalculator()
  extends Receiver[Int](StorageLevel.MEMORY_AND_DISK_2) {

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
        store(scala.util.Random.nextInt(1000));
      }
      Thread.sleep(10)
    }
  }
}
