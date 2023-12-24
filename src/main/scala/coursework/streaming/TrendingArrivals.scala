package coursework.streaming

import java.io.File
import java.util.concurrent.atomic.AtomicInteger
import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{ManualClockWrapper, Minutes, State, StateSpec, StreamingContext, Time}
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.util.LongAccumulator
import org.rogach.scallop._

import scala.collection.mutable

class TrendingArrivalsConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, checkpoint, output)
  val input = opt[String](descr = "input path", required = true)
  val checkpoint = opt[String](descr = "checkpoint path", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}

object TrendingArrivals {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]): Unit = {
    val args = new TrendingArrivalsConf(argv)

    log.info("Input: " + args.input())

    val spark = SparkSession
      .builder()
      .config("spark.streaming.clock", "org.apache.spark.util.ManualClock")
      .appName("TrendingArrivals")
      .getOrCreate()

    val numCompletedRDDs = spark.sparkContext.longAccumulator("number of completed RDDs")

    val batchDuration = Minutes(1)
    val ssc = new StreamingContext(spark.sparkContext, batchDuration)
    val batchListener = new StreamingContextBatchCompletionListener(ssc, 24*6)
    ssc.addStreamingListener(batchListener)

    val rdds = buildMockStream(ssc.sparkContext, args.input())
    val inputData: mutable.Queue[RDD[String]] = mutable.Queue()
    val stream = ssc.queueStream(inputData)

    /**
     * Coordinates of regions that derive the bounding box
     * goldman = [[-74.0141012, 40.7152191], [-74.013777, 40.7152275], [-74.0141027, 40.7138745], [-74.0144185, 40.7140753]]
     * citigroup = [[-74.011869, 40.7217236], [-74.009867, 40.721493], [-74.010140,40.720053], [-74.012083, 40.720267]]
     */

    // (minX, maxX, minY, maxY)
    val goldman = (-74.0144185, -74.013777, 40.7138745, 40.7152275)
    val citigroup = (-74.012083, -74.009867, 40.720053, 40.7217236)

    def isInBoundingBox(point: (Double, Double), boundingBox:(Double, Double, Double, Double) ): Boolean = {
      val (x, y) = point
      val (minX, maxX, minY, maxY) = boundingBox
      x > minX && x < maxX && y > minY && y < maxY
    }

    def updateState(batchTime: Time, key: String, value: Option[Int], state: State[Int]) = {
      val previousCount = state.getOption().getOrElse(0)
      val currentCount = value.getOrElse(0)
      state.update(currentCount)
      if (currentCount >= 10 && currentCount >= 2 * previousCount) {
        val region = if (key.equals("goldman")) "Goldman Sachs" else "Citigroup"
        val output = s"Number of arrivals to $region has doubled from $previousCount to $currentCount at ${batchTime.milliseconds}!"
        println(output)
      }
      Some((key, (currentCount, batchTime.milliseconds, previousCount)))
    }

    val stateSpec = StateSpec.function(updateState _)

    val wc = stream.map(_.split(","))
      .flatMap(tuple => {
        var dropX = 0.0
        var dropY = 0.0
        if (tuple(0).toLowerCase().contains("green")){
          dropX = tuple(8).toDouble
          dropY = tuple(9).toDouble
        } else {
          dropX = tuple(10).toDouble
          dropY = tuple(11).toDouble
        }
        if (isInBoundingBox((dropX, dropY), goldman)) {
          List(("goldman", 1))
        }
        else if (isInBoundingBox((dropX, dropY), citigroup)) List(("citigroup", 1))
        else List()
      })
      .reduceByKeyAndWindow(
        (x: Int, y: Int) => x + y, (x: Int, y: Int) => x - y, Minutes(10), Minutes(10))
      .mapWithState(stateSpec)
      .persist()

    val filePath = args.output() + "/part-"

    wc.foreachRDD { (rdd, time) => {
        val outputPath = filePath + f"${time.milliseconds}%08d"
        rdd.saveAsTextFile(outputPath)
        numCompletedRDDs.add(1L)
      }
    }
    ssc.checkpoint(args.checkpoint())
    ssc.start()

    for (rdd <- rdds) {
      inputData += rdd
      ManualClockWrapper.advanceManualClock(ssc, batchDuration.milliseconds, 50L)
    }

    batchListener.waitUntilCompleted(() =>
      ssc.stop()
    )
  }

  class StreamingContextBatchCompletionListener(val ssc: StreamingContext, val limit: Int) extends StreamingListener {
    def waitUntilCompleted(cleanUpFunc: () => Unit): Unit = {
      while (!sparkExSeen) {}
      cleanUpFunc()
    }

    val numBatchesExecuted = new AtomicInteger(0)
    @volatile var sparkExSeen = false

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
      val curNumBatches = numBatchesExecuted.incrementAndGet()
      log.info(s"${curNumBatches} batches have been executed")
      if (curNumBatches == limit) {
        sparkExSeen = true
      }
    }
  }

  def buildMockStream(sc: SparkContext, directoryName: String): Array[RDD[String]] = {
    val d = new File(directoryName)
    if (d.exists() && d.isDirectory) {
      d.listFiles
        .filter(file => file.isFile && file.getName.startsWith("part-"))
        .map(file => d.getAbsolutePath + "/" + file.getName).sorted
        .map(path => sc.textFile(path))
    } else {
      throw new IllegalArgumentException(s"$directoryName is not a valid directory containing part files!")
    }
  }
}
