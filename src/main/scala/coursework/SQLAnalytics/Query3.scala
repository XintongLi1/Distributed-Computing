package coursework.SQLAnalytics

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD._
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Conf3(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path of data", required = true)
  val date = opt[String](descr = "l_shipdate predicate", required = true)
  val text = opt[Boolean](descr = "work with plaintext data", required = false, default = Some(false))
  val parquet = opt[Boolean](descr = "work with Parquet data", required = false, default = Some(false))

  verify()
}

object Query3{
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf3(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    if (args.text()) log.info("Works with the plaintext data")
    if (args.parquet()) log.info("Works with the Parquet data")

    val conf = new SparkConf().setAppName("SQL Query3")
    val sc = new SparkContext(conf)

    val date = args.date()

    /**
     * select l_orderkey, p_name, s_name from lineitem, part, supplier
     * where
     * l_partkey = p_partkey and
     * l_suppkey = s_suppkey and
     * l_shipdate = 'YYYY-MM-DD'
     * order by l_orderkey asc limit 20;
     */

    if (args.text()){
      val supplier = sc.broadcast(sc.textFile(args.input() + "/supplier.tbl")
        .map(line => {
          val row = line.split('|')
          val suppkey = row(0)
          val name = row(1)
          (suppkey, name)
        })
        .collectAsMap()
      )

      val part = sc.broadcast(sc.textFile(args.input() + "/part.tbl")
        .map (line => {
          val row = line.split('|')
          val partkey = row(0)
          val name = row(1)
          (partkey, name)
        })
        .collectAsMap()
      )

      val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
        .flatMap(line => {
          val row = line.split('|')
          val orderkey = row(0)
          val shipdate = row(10)
          val l_partkey = row(1)
          val l_suppkey = row(2)
          if (shipdate.trim == date && supplier.value.contains(l_suppkey) && part.value.contains(l_partkey)) {
            List((orderkey.toInt,
                  part.value(l_partkey),
                  supplier.value(l_suppkey)
                ))
          }
          else List()
        })

      val query = lineitem
        .sortBy(_._1)
        .take(20)

      query.foreach { case (orderkey, pname, sname) =>
        println(s"($orderkey,$pname,$sname)")
      }
    }

    if (args.parquet()){
      val sparkSession = SparkSession.builder.getOrCreate
      val partDF = sparkSession.read.parquet(args.input() + "/part")
      val part = sc.broadcast(partDF.rdd
        .map(row => {
          val partkey = row(0)
          val name = row(1)
          (partkey, name)
        })
        .collectAsMap()
      )

      val supplierDF = sparkSession.read.parquet(args.input() + "/supplier")
      val supplier = sc.broadcast(supplierDF.rdd
        .map(row => {
          val suppkey = row(0)
          val name = row(1)
          (suppkey, name)
        })
        .collectAsMap()
      )

      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
        .flatMap(row => {
          val orderkey = row(0)
          val shipdate = row(10)
          val l_partkey = row(1)
          val l_suppkey = row(2)
          if (shipdate.toString.trim == date && supplier.value.contains(l_suppkey) && part.value.contains(l_partkey)) {
            List((orderkey.toString.toInt,
              part.value(l_partkey),
              supplier.value(l_suppkey)
            ))
          }
          else List()
        })

      val query = lineitemRDD
        .sortBy(_._1)
        .take(20)

      query.foreach { case (orderkey, pname, sname) =>
        println(s"($orderkey,$pname,$sname)")
      }
    }
  }
}