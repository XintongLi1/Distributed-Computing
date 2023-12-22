package coursework.SQLAnalytics

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD._
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Conf6(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path of data", required = true)
  val date = opt[String](descr = "l_shipdate predicate", required = true)
  val text = opt[Boolean](descr = "work with plaintext data", required = false, default = Some(false))
  val parquet = opt[Boolean](descr = "work with Parquet data", required = false, default = Some(false))

  verify()
}

object Query6{
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf6(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    if (args.text()) log.info("Works with the plaintext data")
    if (args.parquet()) log.info("Works with the Parquet data")

    val conf = new SparkConf().setAppName("SQL Query6")
    val sc = new SparkContext(conf)

    val date = args.date()

    /**
     * select
     * l_returnflag,
     * l_linestatus,
     * sum(l_quantity) as sum_qty,
     * sum(l_extendedprice) as sum_base_price,
     * sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
     * sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
     * avg(l_quantity) as avg_qty,
     * avg(l_extendedprice) as avg_price,
     * avg(l_discount) as avg_disc,
     * count(*) as count_order
     * from lineitem
     * where
     * l_shipdate = 'YYYY-MM-DD'
     * group by l_returnflag, l_linestatus;
     */

    if (args.text()){
      val query = sc.textFile(args.input() + "/lineitem.tbl")
        .flatMap(line => {
          val row = line.split('|')
          val shipdate = row(10)
          if (shipdate.trim == date) {
            val returnflag = row(8)
            val linestatus = row(9)
            val quantity = row(4).toDouble
            val extendedprice = row(5).toDouble
            val discount = row(6).toDouble
            val tax = row(7).toDouble
            val disc_price = extendedprice * (1 - discount)
            val charge = extendedprice * (1 - discount) * (1 + tax)
            List(((returnflag, linestatus), (quantity, extendedprice, disc_price, charge, discount, 1)))
          }
          else List()
        })
        .reduceByKey((v1, v2) => {
          (v1._1 + v2._1, v1._2 + v2._2, v1._3 + v2._3, v1._4 + v2._4, v1._5 + v2._5, v1._6 + v2._6)
        })
        .map (pair => {
          val key = pair._1
          val value = pair._2
          val returnflag = key._1
          val linestatus = key._2
          val count = value._6
          val sum_qty = value._1
          val sum_base_price = value._2
          val sum_disc_price = value._3
          val sum_charge = value._4
          val avg_qty = sum_qty / count
          val avg_price = sum_base_price / count
          val avg_disc = value._5 / count
          (returnflag, linestatus, sum_qty, sum_base_price, sum_disc_price, sum_charge, avg_qty, avg_price, avg_disc, count)
        })
        .collect()
        .foreach(println)
    }

    if (args.parquet()){
      val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val query = lineitemDF.rdd
        .flatMap(row => {
          val shipdate = row(10)
          if (shipdate.toString.trim == date){
            val returnflag = row(8)
            val linestatus = row(9)
            val quantity = row(4).toString.toDouble
            val extendedprice = row(5).toString.toDouble
            val discount = row(6).toString.toDouble
            val tax = row(7).toString.toDouble
            val disc_price = extendedprice * (1 - discount)
            val charge = extendedprice * (1 - discount) * (1 + tax)
            List(((returnflag, linestatus), (quantity, extendedprice, disc_price, charge, discount, 1)))
          }
          else List()
        })
        .reduceByKey((v1, v2) => {
          (v1._1 + v2._1, v1._2 + v2._2, v1._3 + v2._3, v1._4 + v2._4, v1._5 + v2._5, v1._6 + v2._6)
        })
        .map(pair => {
          val key = pair._1
          val value = pair._2
          val returnflag = key._1
          val linestatus = key._2
          val count = value._6
          val sum_qty = value._1
          val sum_base_price = value._2
          val sum_disc_price = value._3
          val sum_charge = value._4
          val avg_qty = sum_qty / count
          val avg_price = sum_base_price / count
          val avg_disc = value._5 / count
          (returnflag, linestatus, sum_qty, sum_base_price, sum_disc_price, sum_charge, avg_qty, avg_price, avg_disc, count)
        })
        .collect()
        .foreach(println)
    }
  }
}