package coursework.SQLAnalytics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object VerifySQL {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("CSV Query")
      .getOrCreate()

    try {
      val shipDate = "1996-01-01"
      // Path of all tables
      val inputPath = "TPC-H-0.1-TXT"

      val lineitemPath = inputPath + "/lineitem.tbl"
      val ordersPath = inputPath + "/orders.tbl"
      val customerPath = inputPath + "/customer.tbl"
      val nationPath = inputPath + "/nation.tbl"
      val partPath = inputPath + "/part.tbl"
      val partsuppPath = inputPath + "/partsupp.tbl"
      val supplierPath = inputPath + "/supplier.tbl"
      val regionPath = inputPath + "/region.tbl"

      // Define schema for the Customer table
      val customerSchema = StructType(Array(
        StructField("CUSTKEY", IntegerType, false),
        StructField("NAME", StringType, true),
        StructField("ADDRESS", StringType, true),
        StructField("NATIONKEY", IntegerType, true),
        StructField("PHONE", StringType, true),
        StructField("ACCTBAL", DecimalType(10, 2), true),
        StructField("MKTSEGMENT", StringType, true),
        StructField("COMMENT", StringType, true)
      ))

      // Define schema for the LineItem table
      val lineitemSchema = StructType(Array(
        StructField("ORDERKEY", IntegerType, false),
        StructField("PARTKEY", IntegerType, true),
        StructField("SUPPKEY", IntegerType, true),
        StructField("LINENUMBER", IntegerType, true),
        StructField("QUANTITY", DecimalType(10, 2), true),
        StructField("EXTENDEDPRICE", DecimalType(10, 2), true),
        StructField("DISCOUNT", DecimalType(10, 2), true),
        StructField("TAX", DecimalType(10, 2), true),
        StructField("RETURNFLAG", StringType, true),
        StructField("LINESTATUS", StringType, true),
        StructField("SHIPDATE", DateType, true),
        StructField("COMMITDATE", DateType, true),
        StructField("RECEIPTDATE", DateType, true),
        StructField("SHIPINSTRUCT", StringType, true),
        StructField("SHIPMODE", StringType, true),
        StructField("COMMENT", StringType, true)
      ))

      // Define schema for the Nation table
      val nationSchema = StructType(Array(
        StructField("NATIONKEY", IntegerType, false),
        StructField("NAME", StringType, true),
        StructField("REGIONKEY", IntegerType, true),
        StructField("COMMENT", StringType, true)
      ))

      // Define schema for the Orders table
      val ordersSchema = StructType(Array(
        StructField("ORDERKEY", IntegerType, false),
        StructField("CUSTKEY", IntegerType, true),
        StructField("ORDERSTATUS", StringType, true),
        StructField("TOTALPRICE", DecimalType(10, 2), true),
        StructField("ORDERDATE", DateType, true),
        StructField("ORDERPRIORITY", StringType, true),
        StructField("CLERK", StringType, true),
        StructField("SHIPPRIORITY", IntegerType, true),
        StructField("COMMENT", StringType, true)
      ))

      // Define schema for the Part table
      val partSchema = StructType(Array(
        StructField("PARTKEY", IntegerType, false),
        StructField("NAME", StringType, true),
        StructField("MFGR", StringType, true),
        StructField("BRAND", StringType, true),
        StructField("TYPE", StringType, true),
        StructField("SIZE", IntegerType, true),
        StructField("CONTAINER", StringType, true),
        StructField("RETAILPRICE", DecimalType(10, 2), true),
        StructField("COMMENT", StringType, true)
      ))

      // Define schema for the PartSupp table
      val partsuppSchema = StructType(Array(
        StructField("PARTKEY", IntegerType, false),
        StructField("SUPPKEY", IntegerType, true),
        StructField("AVAILQTY", IntegerType, true),
        StructField("SUPPLYCOST", DecimalType(10, 2), true),
        StructField("COMMENT", StringType, true)
      ))

      // Define schema for the Region table
      val regionSchema = StructType(Array(
        StructField("REGIONKEY", IntegerType, false),
        StructField("NAME", StringType, true),
        StructField("COMMENT", StringType, true)
      ))

      // Define schema for the Supplier table
      val supplierSchema = StructType(Array(
        StructField("SUPPKEY", IntegerType, false),
        StructField("NAME", StringType, true),
        StructField("ADDRESS", StringType, true),
        StructField("NATIONKEY", IntegerType, true),
        StructField("PHONE", StringType, true),
        StructField("ACCTBAL", DecimalType(10, 2), true),
        StructField("COMMENT", StringType, true)
      ))


      // Read the Customer CSV file and create a temporary view
      val customer = spark.read.format("csv")
        .schema(customerSchema)
        .option("delimiter", "|")
        .load(customerPath)
      customer.createOrReplaceTempView("customer")

      // Read the LineItem CSV file and create a temporary view
      val lineitem = spark.read.format("csv")
        .schema(lineitemSchema)
        .option("delimiter", "|")
        .load(lineitemPath)
      lineitem.createOrReplaceTempView("lineitem")

      // Read the Nation CSV file and create a temporary view
      val nation = spark.read.format("csv")
        .schema(nationSchema)
        .option("delimiter", "|")
        .load(nationPath)
      nation.createOrReplaceTempView("nation")

      // Read the Orders CSV file and create a temporary view
      val orders = spark.read.format("csv")
        .schema(ordersSchema)
        .option("delimiter", "|")
        .load(ordersPath)
      orders.createOrReplaceTempView("orders")

      // Read the Part CSV file and create a temporary view
      val part = spark.read.format("csv")
        .schema(partSchema)
        .option("delimiter", "|")
        .load(partPath)
      part.createOrReplaceTempView("part")

      // Read the PartSupp CSV file and create a temporary view
      val partsupp = spark.read.format("csv")
        .schema(partsuppSchema)
        .option("delimiter", "|")
        .load(partsuppPath)
      partsupp.createOrReplaceTempView("partsupp")

      // Read the Region CSV file and create a temporary view
      val region = spark.read.format("csv")
        .schema(regionSchema)
        .option("delimiter", "|")
        .load(regionPath)
      region.createOrReplaceTempView("region")

      // Read the Supplier CSV file and create a temporary view
      val supplier = spark.read.format("csv")
        .schema(supplierSchema)
        .option("delimiter", "|")
        .load(supplierPath)
      supplier.createOrReplaceTempView("supplier")

      // Run SQL queries
      // Q2
      // spark.sql(
      //   s"""
      //   |select o.clerk, o.orderkey from lineitem l, orders o
      //   |where
      //   |  l.orderkey = o.orderkey and
      //   |  l.shipdate = '$shipDate'
      //   |order by o.orderkey asc limit 20
      //    """.stripMargin
      // ).show(100)

      // Q4
//      spark.sql(
//        s"""
//           |SELECT n.NATIONKEY as n_nationkey, n.NAME as n_name, count(*) as count
//           |FROM lineitem l, orders o, customer c, nation n
//           |WHERE l.ORDERKEY = o.ORDERKEY
//           | AND  o.CUSTKEY = c.CUSTKEY
//           | AND  c.NATIONKEY = n.NATIONKEY
//           | AND  l.SHIPDATE = '$shipDate'
//           |GROUP BY n.NATIONKEY, n.NAME
//           |ORDER BY n.NATIONKEY ASC
//            """.stripMargin).show(100)

      // Q6
     spark.sql(
       s"""
          |select
          |  l.returnflag,
          |  l.linestatus,
          |  sum(l.quantity) as sum_qty,
          |  sum(l.extendedprice) as sum_base_price,
          |  sum(l.extendedprice*(1-l.discount)) as sum_disc_price,
          |  sum(l.extendedprice*(1-l.discount)*(1+l.tax)) as sum_charge,
          |  avg(l.quantity) as avg_qty,
          |  avg(l.extendedprice) as avg_price,
          |  avg(l.discount) as avg_disc,
          |  count(*) as count_order
          |from lineitem l
          |where
          |  l.shipdate = '$shipDate'
          |group by l.returnflag, l.linestatus
          |""".stripMargin
     ).show(100)

    } finally {
      // Stop the SparkSession
      spark.stop()
    }
  }
}
