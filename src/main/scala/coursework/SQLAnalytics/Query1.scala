package coursework.SQLAnalytics

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD._
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Conf1(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path of data", required = true)
  val date = opt[String](descr = "l_shipdate predicate", required = true)
  val text = opt[Boolean](descr = "work with plaintext data", required = false, default = Some(false))
  val parquet = opt[Boolean](descr = "work with Parquet data", required = false, default = Some(false))

  verify()
}

object Query1{
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf1(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    if (args.text()) log.info("Works with the plaintext data")
    if (args.parquet()) log.info("Works with the Parquet data")

    val conf = new SparkConf().setAppName("SQL Query1")
    val sc = new SparkContext(conf)

    val date = args.date()    // shipdate

    // works with lineitem table
    if (args.text()){
      val query = sc.textFile(args.input() + "/lineitem.tbl")
        .filter(line => {
          val row = line.split('|')
          val shipdate = row(10)
          shipdate.trim == date
        })
        .count()
      println("ANSWER=" + query)
    }
    if (args.parquet()){
      val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val query = lineitemRDD
        .filter(line => {
          val shipdate = line(10)
          shipdate.toString.trim == date
        })
        .count()
      println("ANSWER=" + query)
    }
  }
}