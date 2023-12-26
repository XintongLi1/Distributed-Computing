package coursework.spamClassifier

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD._
import org.rogach.scallop._
import scala.collection.mutable.Map

class Conf0(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model, shuffle)
  val input = opt[String](descr = "input path of training instances", required = true)
  val model = opt[String](descr = "output path of model", required = true)
  val shuffle = opt[Boolean](descr = "enable shuffling of the training data", required = false, default = Some(false))
  verify()
}

object TrainSpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf0(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.model())
    log.info("Shuffle: " + args.shuffle())

    var dataset = "???"
    if (args.input().contains("group_x")) dataset = "group x"
    else if (args.input().contains("group_y")) dataset = "group y"
    else if (args.input().contains("train.all")) dataset = "all"
    else {
      if (args.shuffle()) dataset = "shuffle britney"
      else dataset = "britney"
    }

    val conf = new SparkConf().setAppName("TrainSpamClassifier - " + dataset)
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.model())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val doShuffle = args.shuffle()

    val textFile = sc.textFile(args.input(), 1)

    var preTrain = textFile.map(line => {
      // Parse input
      val data = line.trim.split("\\s+")
      val docid = data(0)
      val isSpam = if (data(1) == "spam") 1 else 0
      val features = data.drop(2).map(s => s.toInt)
      val randKey = scala.util.Random.nextInt()
      (0, (docid, isSpam, features, randKey))
    })
    if (doShuffle){
      preTrain = preTrain.sortBy(_._2._4)
    }

    val trained = preTrain.groupByKey(1)
      // Then run the trainer...
      .flatMap(kvPair => {
        val trainData = kvPair._2
        val weightVector = Map[Int, Double]()

        // Scores a document based on its list of features.
        def spamminess(features: Array[Int]): Double = {
          var score = 0d
          features.foreach(f => if (weightVector.contains(f)) score += weightVector(f))
          score
        }

        // This is the main learner:
        val delta = 0.002

        // For each instance
        trainData.foreach(instance => {
          val isSpam = instance._2
          val features = instance._3
          // Update the weights as follows:
          val score = spamminess(features)
          val prob = 1.0 / (1 + Math.exp(-score))
          features.foreach(f => {
            if (weightVector.contains(f)) {
              weightVector(f) += (isSpam - prob) * delta
            } else {
              weightVector(f) = (isSpam - prob) * delta
            }
          })
        })
        weightVector.toList
    })

    trained.saveAsTextFile(args.model())
  }
}
