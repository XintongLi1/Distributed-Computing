package coursework.spamClassifier

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD._
import org.rogach.scallop._

class Conf1(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, model)
  val input = opt[String](descr = "input path of test instances", required = true)
  val output = opt[String](descr = "output path", required = true)
  val model = opt[String](descr = "classifier model", required = true)
  verify()
}
object ApplySpamClassifier  {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf1(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Classifier model: " + args.model())

    val conf = new SparkConf().setAppName("ApplySpamClassifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)


    val modelFile = sc.textFile(args.model() + "/part-00000")
    val weightVector = sc.broadcast(modelFile
      .map(line => {
        val tup = line.trim.drop(1).dropRight(1).split(',')
        (tup(0).toInt, tup(1).toDouble)
      })
        .collectAsMap()
    )

    val textFile = sc.textFile(args.input())
    val tested = textFile.map(line => {
      // Parse input
      val data = line.trim.split("\\s+")
      val docid = data(0)
      val actualType = data(1)
      val features = data.drop(2).map(s => s.toInt)

      // Predict
      def spamminess(features: Array[Int]): Double = {
        var score = 0d
        features.foreach(f => if (weightVector.value.contains(f)) score += weightVector.value(f))
        score
      }

      val score = spamminess(features)
      val predType = if (score > 0) "spam" else "ham"
//      val prob = 1.0 / (1 + Math.exp(-score))
//      assert(predType == (if (prob > 0.5) "spam" else "ham"))

      (docid, actualType, score, predType)
    })

    tested.saveAsTextFile(args.output())

  }
}
