package coursework.SQLAnalytics

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD._
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Conf2(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path of data", required = true)
  val date = opt[String](descr = "l_shipdate predicate", required = true)
  val text = opt[Boolean](descr = "work with plaintext data", required = false, default = Some(false))
  val parquet = opt[Boolean](descr = "work with Parquet data", required = false, default = Some(false))

  verify()
}

object Query2{
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf2(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    if (args.text()) log.info("Works with the plaintext data")
    if (args.parquet()) log.info("Works with the Parquet data")

    val conf = new SparkConf().setAppName("SQL Query2")
    val sc = new SparkContext(conf)

    val date = args.date()

    /**
     * select o_clerk, o_orderkey from lineitem, orders
     * where
     *  l_orderkey = o_orderkey and
     *  l_shipdate = 'YYYY-MM-DD'
     * order by o_orderkey asc limit 20;
     */

    if (args.text()){
      val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
        .flatMap(line => {
          val row = line.split('|')
          val orderkey = row(0)
          val shipdate = row(10)
          if (shipdate.trim == date) List((orderkey.toInt, '*'))
          else List()
        })

      val orders = sc.textFile(args.input() + "/orders.tbl")
        .map(line => {
          val row = line.split('|')
          val orderkey = row(0)
          val clerk = row(6)
          (orderkey.toInt, clerk)
        })

      val query = orders.cogroup(lineitem)
        .flatMap {
          case (orderkey, (o_clerk, l_dummy)) => {
            if (o_clerk.isEmpty || l_dummy.isEmpty) List()
            else l_dummy.map(dummy => (orderkey, o_clerk.head))
          }
        }
        .sortByKey(numPartitions = 1)
        .take(20)

      query.foreach { case (orderkey, clerk) =>
        println(s"($clerk,$orderkey)")
      }
    }

    if (args.parquet()){
      val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
        .flatMap(line => {
          val orderkey = line(0)
          val shipdate = line(10)
          if (shipdate.toString.trim == date) List((orderkey.toString.toInt, '*'))
          else List()
        })

      val ordersDF = sparkSession.read.parquet(args.input() + "/orders")
      val ordersRDD = ordersDF.rdd
        .map(line => {
          val orderkey = line(0)
          val clerk = line(6)
          (orderkey.toString.toInt, clerk)
        })


      val query = ordersRDD.cogroup(lineitemRDD)
        .flatMap {
          case (orderkey, (o_clerk, l_dummy)) => {
            if (o_clerk.isEmpty || l_dummy.isEmpty) List()
            else l_dummy.map(dummy => (orderkey, o_clerk.head))
          }
        }
        .sortByKey(numPartitions = 1)
        .take(20)

      query.foreach { case (orderkey, clerk) =>
        println(s"($clerk,$orderkey)")
      }
    }
  }
}