package coursework.Spark

import io.bespin.scala.util.Tokenizer
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD._
import org.rogach.scallop._

class Conf2(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "threshold of co-occurrence", required = false, default = Some(10))
  verify()
}

class MyPartitionPMI (override val numPartitions: Int) extends Partitioner {
  def getPartition(key: Any): Int = key match {
    case (word1 : String, word2 : String) => { (word1.hashCode & Integer.MAX_VALUE) % numPartitions }
  }
}


object PairsPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf2(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Threshold of co-occurrence: " + args.threshold())

//    val startTime = System.currentTimeMillis()

    val conf = new SparkConf().setAppName("PairsPMI")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val partitioner = new MyPartitionPMI(args.reducers())
    val WORD_LIMIT = 40
    // Conf2 is not made Serializable. Using args._ in flatMap will make the task not serializable
    val threshold = args.threshold()

    val textFile = sc.textFile(args.input())
    val wordCountsRDD = textFile
      .flatMap(line => List("*") ++ tokenize(line).take(WORD_LIMIT).distinct )
      .map(word => (word, 1))
      .reduceByKey(_+_)

    val wordCounts = sc.broadcast(wordCountsRDD.collectAsMap())

    val pointWiseMutualInformation = textFile
      .flatMap(line => {
        val tokens = tokenize(line).take(WORD_LIMIT).distinct
        tokens.flatMap(word1 =>
          tokens.map(word2 => (word1, word2)).filter { case (word1, word2) => !word1.equals(word2)}
        )
      })
      .map(pair => (pair, 1))
      .reduceByKey(partitioner, _+_)
      .repartitionAndSortWithinPartitions(partitioner)
      .flatMap { case ((word1, word2), cnt) =>
        if (cnt < threshold) Nil
        else {
          val numLine = wordCounts.value("*")
          val word1Occurrence = wordCounts.value(word1)
          val word2Occurrence = wordCounts.value(word2)
          val PMI: Float = (math.log10(cnt * 1.0f * numLine / (word1Occurrence * word2Occurrence))).toFloat
          List(((word1, word2), (PMI, cnt)))
        }
      }
    pointWiseMutualInformation.saveAsTextFile(args.output())
//    val duration = (System.currentTimeMillis() - startTime) / 1000.0
//    log.info(s"Job finished in $duration seconds")
  }
}
