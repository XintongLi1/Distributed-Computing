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
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD._
import org.rogach.scallop._

class Conf1(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}

object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf1(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("ComputeBigramRelativeFrequencyStripes")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    val relativeFrequencyStripes = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) {
          val bigrams = tokens.sliding(2).map(bigram => (bigram.head, Map((bigram(1), 1))))
          bigrams.toList
        } else List()
      })
      .reduceByKey(new HashPartitioner(args.reducers()), (map1, map2) => {
        // Merge two maps and sum the values of common keys
        map1 ++ map2.map {
          // If a key exists in both maps, the value from the second map will be taken
          case (k, v) => k -> (v + map1.getOrElse(k, 0))
        }
      })
      .map(stripe => {
        val marginal_count: Float = stripe._2.values.sum
        (stripe._1, stripe._2.map { case (k, v) => k -> v / marginal_count })
    })
    relativeFrequencyStripes.saveAsTextFile(args.output())
  }
}
