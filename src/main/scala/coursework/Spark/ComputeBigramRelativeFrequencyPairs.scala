/**
 * Bespin: reference implementations of "big data" algorithms
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package coursework.Spark

import io.bespin.scala.util.Tokenizer
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD._
import org.rogach.scallop._

class Conf0(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}

class MyPartition (override val numPartitions: Int) extends Partitioner {
  def getPartition(key: Any): Int = key match {
    case (word1 : String, word2 : String) => { (word1.hashCode & Integer.MAX_VALUE) % numPartitions }
  }
}


object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf0(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("ComputeBigramRelativeFrequencyPairs")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    var marginal_count = 0.0f
    val partitioner = new MyPartition(args.reducers())

    val textFile = sc.textFile(args.input())
    val relativeFrequencyPairs = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) {
          val bigrams = tokens.sliding(2).toList
          val wordCount = bigrams.map(bigram => List(bigram(0), "*"))
          wordCount ++ bigrams
        } else List()
      })
      .map(bigram => ((bigram.head, bigram(1)), 1.0f))
      .reduceByKey(partitioner, _ + _)
      .repartitionAndSortWithinPartitions(partitioner)
      .map(bigram => bigram._1 match {
        case (word, "*") => {
          marginal_count = bigram._2
          bigram
        }
        case (word1, word2) => (bigram._1, bigram._2 / marginal_count)
      })
    relativeFrequencyPairs.saveAsTextFile(args.output())
  }
}
