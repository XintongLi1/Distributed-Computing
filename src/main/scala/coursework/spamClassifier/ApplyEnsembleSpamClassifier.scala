package coursework.spamClassifier

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD._
import org.rogach.scallop._


class Conf2(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, model, method)
  val input = opt[String](descr = "input path of test instances", required = true)
  val output = opt[String](descr = "output path", required = true)
  val model = opt[String](descr = "output path of model", required = true)
  val method = opt[String](descr = "ensemble technique", required = true)
  verify()
}

object ApplyEnsembleSpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf2(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Classifier model: " + args.model())
    log.info("Ensemble technique: " + args.method())


    val conf = new SparkConf().setAppName("ApplyEnsembleSpamClassifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val modelGroupX = sc.textFile(args.model() + "/part-00000")
    val modelGroupY = sc.textFile(args.model() + "/part-00001")
    val modelBritney = sc.textFile(args.model() + "/part-00002")
    val weightGroupX = sc.broadcast(modelGroupX
      .map(line => {
        val tup = line.trim.drop(1).dropRight(1).split(',')
        (tup(0).toInt, tup(1).toDouble)
      })
      .collectAsMap()
    )
    val weightGroupY = sc.broadcast(modelGroupY
      .map(line => {
        val tup = line.trim.drop(1).dropRight(1).split(',')
        (tup(0).toInt, tup(1).toDouble)
      })
      .collectAsMap()
    )
    val weightBritney = sc.broadcast(modelBritney
      .map(line => {
        val tup = line.trim.drop(1).dropRight(1).split(',')
        (tup(0).toInt, tup(1).toDouble)
      })
      .collectAsMap()
    )

    val method = args.method()
    val textFile = sc.textFile(args.input())
    val tested = textFile.map(line => {
      // Parse input
      val data = line.trim.split("\\s+")
      val docid = data(0)
      val actualType = data(1)
      val features = data.drop(2).map(s => s.toInt)

      // Predict
      def spamminess(features: Array[Int], weightVector: scala.collection.Map[Int, Double]): Double = {
        var score = 0d
        features.foreach(f => if (weightVector.contains(f)) score += weightVector(f))
        score
      }

      val scoreGroupX = spamminess(features, weightGroupX.value)
      val scoreGroupY = spamminess(features, weightGroupY.value)
      val scoreBritney = spamminess(features, weightBritney.value)
      var ensembleScore = 0d
      if (method.contains("average")){
          ensembleScore = (scoreGroupX + scoreGroupY + scoreBritney) / 3
      } else if (method.contains("vote")) {
          ensembleScore += (if (scoreGroupX > 0) 1 else -1)
          ensembleScore += (if (scoreGroupY > 0) 1 else -1)
          ensembleScore += (if (scoreBritney > 0) 1 else -1)
      } else {
        throw new IllegalArgumentException("The `method` option only accepts \"average\" or \"vote\"")
      }
      val predType = if (ensembleScore > 0) "spam" else "ham"

      (docid, actualType, ensembleScore, predType)
    })

    tested.saveAsTextFile(args.output())
  }
}