package coursework.SQLAnalytics

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD._
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Conf7(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path of data", required = true)
  val date = opt[String](descr = "l_shipdate predicate", required = true)
  val text = opt[Boolean](descr = "work with plaintext data", required = false, default = Some(false))
  val parquet = opt[Boolean](descr = "work with Parquet data", required = false, default = Some(false))

  verify()
}

object Query7{
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf7(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    if (args.text()) log.info("Works with the plaintext data")
    if (args.parquet()) log.info("Works with the Parquet data")

    val conf = new SparkConf().setAppName("SQL Query7")
    val sc = new SparkContext(conf)

    val date = args.date()

    /**
     * select
     * c_name,
     * l_orderkey,
     * sum(l_extendedprice*(1-l_discount)) as revenue,
     * o_orderdate,
     * o_shippriority
     * from customer, orders, lineitem
     * where
     * c_custkey = o_custkey and
     * l_orderkey = o_orderkey and
     * o_orderdate < "YYYY-MM-DD" and
     * l_shipdate > "YYYY-MM-DD"
     * group by
     * c_name,
     * l_orderkey,
     * o_orderdate,
     * o_shippriority
     * order by
     * revenue desc
     * limit 5;
     */

    if (args.text()){
      val customer = sc.broadcast(sc.textFile(args.input() + "/customer.tbl")
        .map(line => {
          val row = line.split('|')
          val custkey = row(0)
          val c_name = row(1)
          (custkey, c_name)
        })
        .collectAsMap()
      )

      val orders = sc.textFile(args.input() + "/orders.tbl")
        .flatMap(line => {
          val row = line.split('|')
          val orderkey = row(0)
          val custkey = row(1)
          val orderdate = row(4)
          val shippriority = row(7)
          if (orderdate.trim < date && customer.value.contains(custkey)) {
            val c_name = customer.value(custkey)
            List((orderkey, (c_name, orderdate, shippriority)))
          }
          else List()
        })

      val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
        .flatMap(line => {
          val row = line.split('|')
          val shipdate = row(10)
          if (shipdate.trim > date){
            val orderkey = row(0)
            val extendedprice = row(5).toDouble
            val discount = row(6).toDouble
            val revenue = extendedprice*(1-discount)
            List((orderkey, revenue))
          }
          else List()
        })

      val query = orders.cogroup(lineitem)
        .filter {
          case (orderkey, (o_value, l_revenue)) => o_value.nonEmpty && l_revenue.nonEmpty
        }
        .flatMap {
          case (orderkey, (o_value, l_revenue)) => {
            val c_name = o_value.head._1
            val orderdate = o_value.head._2
            val shippriority = o_value.head._3
            l_revenue.map(revenue => ((c_name, orderkey, orderdate, shippriority), revenue))
          }
        }
        .reduceByKey(_+_)
        .sortBy(_._2, ascending = false)
        .take(5)
        .map(pair => {
          val key = pair._1
          val revenue = pair._2
          (key._1, key._2, revenue, key._3, key._4)
        })
        .foreach(println)
    }

    if (args.parquet()){
      val sparkSession = SparkSession.builder.getOrCreate
      val customerDF = sparkSession.read.parquet(args.input() + "/customer")
      val customer = sc.broadcast(customerDF.rdd
        .map(row => {
          val custkey = row(0)
          val c_name = row(1)
          (custkey, c_name)
        })
        .collectAsMap()
      )

      val ordersDF = sparkSession.read.parquet(args.input() + "/orders")
      val ordersRDD = ordersDF.rdd
        .flatMap(row => {
          val orderkey = row(0)
          val custkey = row(1)
          val orderdate = row(4)
          val shippriority = row(7)
          if (orderdate.toString.trim < date && customer.value.contains(custkey)) {
            val c_name = customer.value(custkey)
            List((orderkey, (c_name, orderdate, shippriority)))
          }
          else List()
        })

      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
        .flatMap(row => {
          val shipdate = row(10)
          if (shipdate.toString.trim > date) {
            val orderkey = row(0)
            val extendedprice = row(5).toString.toDouble
            val discount = row(6).toString.toDouble
            val revenue = extendedprice * (1 - discount)
            List((orderkey, revenue))
          }
          else List()
        })

      val query = ordersRDD.cogroup(lineitemRDD)
        .filter {
          case (orderkey, (o_value, l_revenue)) => o_value.nonEmpty && l_revenue.nonEmpty
        }
        .flatMap {
          case (orderkey, (o_value, l_revenue)) => {
            val c_name = o_value.head._1
            val orderdate = o_value.head._2
            val shippriority = o_value.head._3
            l_revenue.map(revenue => ((c_name, orderkey, orderdate, shippriority), revenue))
          }
        }
        .reduceByKey(_ + _)
        .sortBy(_._2, ascending = false)
        .take(5)
        .map(pair => {
          val key = pair._1
          val revenue = pair._2
          (key._1, key._2, revenue, key._3, key._4)
        })
        .foreach(println)
    }
  }
}