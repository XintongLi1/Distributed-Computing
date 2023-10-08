package coursework.Spark

import io.bespin.scala.util.Tokenizer
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD._
import org.rogach.scallop._

class Conf3(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "threshold of co-occurrence", required = false, default = Some(10))
  verify()
}


object StripesPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf3(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Threshold of co-occurrence: " + args.threshold())

    val conf = new SparkConf().setAppName("StripesPMI")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val WORD_LIMIT = 40
    // Conf3 is not made Serializable. Using args._ in flatMap will make the task not serializable
    val threshold = args.threshold()

    val textFile = sc.textFile(args.input())
    val wordCountsRDD = textFile
      .flatMap(line => List("*") ++ tokenize(line).take(WORD_LIMIT).distinct)
      .map(word => (word, 1))
      .reduceByKey(_+_)

    val wordCounts = sc.broadcast(wordCountsRDD.collectAsMap())

    val pointWiseMutualInformation = textFile
      .flatMap(line => {
        val tokens = tokenize(line).take(WORD_LIMIT).distinct
        tokens.flatMap(word1 =>
          tokens.flatMap(word2 => if (word1 == word2) Nil else List((word1, Map((word2, 1)))))
        )
      })
      .reduceByKey(new HashPartitioner(args.reducers()), (map1, map2) => {
        // Merge two maps and sum the values of common keys
        map1 ++ map2.map {
          // If a key exists in both maps, the value from the second map will be taken
          case (k, v) => k -> (v + map1.getOrElse(k, 0))
        }
      })
      .flatMap(stripe => {
        val word1 = stripe._1
        val wordMap = stripe._2
        val word1Occurrence = wordCounts.value(word1)
        val newMap = wordMap.flatMap {case (word2, cnt) =>
          if (cnt < threshold) Nil
          else {
            val numLine = wordCounts.value("*")
            val word2Occurrence = wordCounts.value(word2)
            val PMI: Float = (math.log10(cnt * 1.0f * numLine / (word1Occurrence * word2Occurrence))).toFloat
            List((word2, (PMI, cnt)))
          }
        }
        if (newMap.isEmpty) Nil
        else {
          val resMap : Map[String, (Float, Int)] = newMap
          List((word1, resMap))
        }
      })
    pointWiseMutualInformation.saveAsTextFile(args.output())
  }
}
