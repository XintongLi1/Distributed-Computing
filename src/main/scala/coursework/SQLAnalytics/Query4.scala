package coursework.SQLAnalytics

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD._
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Conf4(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path of data", required = true)
  val date = opt[String](descr = "l_shipdate predicate", required = true)
  val text = opt[Boolean](descr = "work with plaintext data", required = false, default = Some(false))
  val parquet = opt[Boolean](descr = "work with Parquet data", required = false, default = Some(false))

  verify()
}

object Query4{
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf4(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    if (args.text()) log.info("Works with the plaintext data")
    if (args.parquet()) log.info("Works with the Parquet data")

    val conf = new SparkConf().setAppName("SQL Query4")
    val sc = new SparkContext(conf)

    val date = args.date()

    /**
     * select n_nationkey, n_name, count(*) from lineitem, orders, customer, nation
     * where
     * l_orderkey = o_orderkey and
     * o_custkey = c_custkey and
     * c_nationkey = n_nationkey and
     * l_shipdate = 'YYYY-MM-DD'
     * group by n_nationkey, n_name
     * order by n_nationkey asc;
     */

    if (args.text()){
      val nation = sc.broadcast(sc.textFile(args.input() + "/nation.tbl")
        .map(line => {
          val row = line.split('|')
          val nationkey = row(0)
          val name = row(1)
          (nationkey.toInt, name)
        })
        .collectAsMap()
      )
      val customer = sc.broadcast(sc.textFile(args.input() + "/customer.tbl")
        .flatMap(line => {
          val row = line.split('|')
          val custkey = row(0)
          val nationkey = row(3).toInt
          if (nation.value.contains(nationkey))
            List((custkey, (nationkey, nation.value(nationkey))))
          else List()
        })
        .collectAsMap()
      )

      val orders = sc.textFile(args.input() + "/orders.tbl")
        .flatMap(line => {
          val row = line.split('|')
          val orderkey = row(0)
          val custkey = row(1)
          if (customer.value.contains(custkey)) {
            val nationkey = customer.value(custkey)._1
            val nationname = customer.value(custkey)._2
            List((orderkey.toInt, (nationkey, nationname)))
          }
          else List()
        })


      val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
        .flatMap(line => {
          val row = line.split('|')
          val orderkey = row(0)
          val shipdate = row(10)
          if (shipdate.trim == date) List((orderkey.toInt, '*'))
          else List()
        })

      val query = orders.cogroup(lineitem)
        .filter {
          case (orderkey, (o_nation, l_dummy)) => o_nation.nonEmpty && l_dummy.nonEmpty
        }
        .map {
          case (orderkey, (o_nation, l_dummy)) => {
            val count = l_dummy.size
            val nationkey = o_nation.head._1
            val nationname = o_nation.head._2
            ((nationkey, nationname), count)
          }
        }
        .reduceByKey(_+_)
        .sortByKey(numPartitions = 1)
        .collect()

      query.foreach { case ((nationkey, nationname), count) =>
        println(s"($nationkey,$nationname,$count)")
      }
    }

    if (args.parquet()){
      val sparkSession = SparkSession.builder.getOrCreate
      val nationDF = sparkSession.read.parquet(args.input() + "/nation")
      val nation = sc.broadcast(nationDF.rdd
        .map(row => {
          val nationkey = row(0)
          val name = row(1)
          (nationkey.toString.toInt, name)
        })
        .collectAsMap()
      )

      val customerDF = sparkSession.read.parquet(args.input() + "/customer")
      val customer = sc.broadcast(customerDF.rdd
        .flatMap(row => {
          val custkey = row(0)
          val nationkey = row(3).toString.toInt
          if (nation.value.contains(nationkey))
            List((custkey, (nationkey, nation.value(nationkey))))
          else List()
        })
        .collectAsMap()
      )

      val ordersDF = sparkSession.read.parquet(args.input() + "/orders")
      val ordersRDD = ordersDF.rdd
        .flatMap(row => {
          val orderkey = row(0)
          val custkey = row(1)
          if (customer.value.contains(custkey)) {
            val nationkey = customer.value(custkey)._1
            val nationname = customer.value(custkey)._2
            List((orderkey.toString.toInt, (nationkey, nationname)))
          }
          else List()
        })

      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
        .flatMap(row => {
        val orderkey = row(0)
        val shipdate = row(10)
        if (shipdate.toString.trim == date) List((orderkey.toString.toInt, '*'))
        else List()
      })

      val query = ordersRDD.cogroup(lineitemRDD)
        .filter {
          case (orderkey, (o_nation, l_dummy)) => o_nation.nonEmpty && l_dummy.nonEmpty
        }
        .map {
          case (orderkey, (o_nation, l_dummy)) => {
            val count = l_dummy.size
            val nationkey = o_nation.head._1
            val nationname = o_nation.head._2
            ((nationkey, nationname), count)
          }
        }
        .reduceByKey(_ + _)
        .sortBy(_._1._1, numPartitions = 1)
        .collect()

      query.foreach { case ((nationkey, nationname), count) =>
        println(s"($nationkey,$nationname,$count)")
      }
    }
  }
}