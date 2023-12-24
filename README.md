# Distributed-Computing
MapReduce, Spark, Inverted texting and Search, Graph Algorithms, Data Mining and ML, Real-time Analytics, etc

All projects are conveniently built using the `mvn clean package` command.

- [Distributed-Computing](#distributed-computing)
  - [Coursework 1: Pointwise Mutual Information (PMI) with MapReduce](#coursework-1-pointwise-mutual-information-pmi-with-mapreduce)
  - [Coursework 2: Computation using Spark](#coursework-2-computation-using-spark)
  - [Coursework 3: Inverted Indexing](#coursework-3-inverted-indexing)
    - [Key Features](#key-features)
      - [Delta-Compressed Inverted Indexing](#delta-compressed-inverted-indexing)
      - [Scalable Postings Buffering](#scalable-postings-buffering)
      - [Boolean Retrieval](#boolean-retrieval)
  - [Coursework 4: PageRank](#coursework-4-pagerank)
    - [Implementation Overview](#implementation-overview)
  - [Coursework 6: SQL Data Analytics](#coursework-6-sql-data-analytics)
  - [Coursework 7: Spark Streaming](#coursework-7-spark-streaming)



## Coursework 1: Pointwise Mutual Information (PMI) with MapReduce 

**Directory:** `src/main/java/coursework/MapReduce`

**About PMI**

The Pointwise Mutual Information (PMI) between two events x and y is calculated as: \
$PMI(x, y) = \log\frac{p(x, y)}{p(x)p(y)}$, \
where $p(x)$ is the probability of the event $x$.

Specifically, in this context, we are interested in the occurrence of x on a line in the file (denominator) and the co-occurrence of x and y on the same line (numerator).

---


This project features two implementations designed to calculate the PMI of words within the provided text file.

* **Pairs Approach**
  * Emit intermediate key-value pairs with each co-occurring word pair as the key and the count (integer one) as the value.
  * Each pair corresponds to a cell in the word co-occurrence matrix.
    `emit ((u, v), 1)`

* **Stripes Approach**
  * Co-occurring word pairs are stored first in an associative array, denoted H.
  * Mappers emit key-value pairs with words as keys and the associative arrays as values. 
  * Each final key-value pair encodes a row in the co-occurrence matrix.
    `emit ((a, {b1:c1, b2:c2, ..., bm:cm}))`, where:
    `a` is a word from the input
    `b1 ... bm` are all words that coöccur with `a`
    `ci` is the number of times `(a, bi)` coöccur
    `{}` means a map (aka a dictionary, associative array, etc)

**Execution commands:**

```bash
hadoop jar target/assignments-1.0.jar coursework.MapReduce.PairsPMI \
   -input data/Shakespeare.txt -output mapReduce-shakespeare-pmi-pairs \
   -reducers 5 -threshold 10
```


## Coursework 2: Computation using Spark

**Directory:** `src/main/scala/coursework/Spark`

This project has two main parts:

1. **Bigram Relative Frequency:**
   * Compute the relative frequency of bigram pairs.
   * This involved porting an [existing implementation](https://github.com/lintool/bespin/tree/master/src/main/java/io/bespin/java/mapreduce/bigram) over to Spark.
2. **PMI with Spark:**
   * A Spark-based implementation of computing PMI which is originally implemented with MapReduce.


**Execution commands:**

```bash
spark-submit --class coursework.Spark.ComputeBigramRelativeFrequencyPairs \
   target/assignments-1.0.jar --input data/Shakespeare.txt \
   --output spark-shakespeare-bigrams-pairs --reducers 5
```


```bash
spark-submit --class coursework.Spark.StripesPMI \
   target/assignments-1.0.jar --input data/Shakespeare.txt \
   --output spark-shakespeare-pmi-stripes --reducers 5 --threshold 10
```

## Coursework 3: Inverted Indexing

**Directory:** `src/main/java/coursework/InvertedIndexing`

This project consists of two parts:
1. Build inverted index for vast amounts of textual data
2. Implement boolean retrieval on top of inverted index.

### Key Features 
#### Delta-Compressed Inverted Indexing
* The inverted index maps each word to a posting list detailing every document the word appears in.
* For efficient storage, we use delta-compression techniques for the docIDs.
  * Instead of storing complete document IDs, the differences between consecutive IDs are encoded and saved.
* Use Variable Integer Encoding (`VInts`) from the `org.apache.hadoop.io.WritableUtils package` to compress term frequencies.

#### Scalable Postings Buffering
* With dynamic partitioning, the system can split the indexed data across multiple reducers. 

#### Boolean Retrieval

The class `BooleanRetrievalCompressed` processes the query against the inverted index. Using Term-at-a-Time Retrieval, it reads the posting list of each queried word and identifies the matching documents or lines.

A sample query is "white red OR rose AND pluck AND". The class searches for documents that contain either the word "white" or "red" (at least one of them), but must also include both "rose" and "pluck".

**Execution commands:**

```bash
hadoop jar target/assignments-1.0.jar coursework.InvertedIndexing.BuildInvertedIndexCompressed \
   -input data/Shakespeare.txt -output inverted-index-shakespeare -reducers 6
```


```bash
hadoop jar target/assignments-1.0.jar coursework.InvertedIndexing.BooleanRetrievalCompressed \
   -index inverted-index-shakespeare -collection data/Shakespeare.txt \
   -query "outrageous fortune AND"

hadoop jar target/assignments-1.0.jar coursework.InvertedIndexing.BooleanRetrievalCompressed \
   -index inverted-index-shakespeare -collection data/Shakespeare.txt \
   -query "white red OR rose AND pluck AND"
```


## Coursework 4: PageRank

**Directory:** `src/main/java/coursework/PageRank`

This project calculates the PageRank mass for all nodes in a given graph and identifies the top nodes with the highest PageRank mass.

Unlike traditional PageRank, which distributes mass uniformly across all nodes, here, the mass is initially divided among a specified set of source nodes (each receiving a mass of 1/m, where m is the number of source nodes), with all other nodes starting at zero. 

A key feature of our model is handling random jumps: whenever a jump occurs (be it random or from a dangling node), it's directed back to one of the source nodes, following a 1/m probability distribution. This is a deviation from the conventional PageRank approach, where random jumps are equally probable to any node.

### Implementation Overview

The java program `BuildPersonalizedPageRankRecord.java` serves as the main driver for running the page rank algorithm.

It works by iteratively computing the mass of each node, integrating the handling of random walks, jumps, and dead-ends into a single pass (one Mapper job, one phase in each iteration).


**Map Phase:**
* Each node calculates the amount of PageRank mass to distribute among its neighbors (listed in its adjacency list).
* The computed PageRank mass is then sent to each of the neighbors, keyed by their node IDs.
* `Emit(neighborID, intermediateMass)`

**Combiner:**
* Sums partial PageRank contributions and passes node structure along

**Reduce Phase:**
* The new PageRank value for each node is set as the sum of all the PageRank masses it receives.
* Random walk is also handled here

As with the parallel breadth-first search algorithm, the graph structure itself must be passed from iteration to iteration. Each node data structure is emitted in the mapper and written back out to disk in the reducer.


**Execution commands:**

Create the initial PageRank graph from adjacency lists

```bash
hadoop jar target/assignments-1.0.jar \
   coursework.PageRank.BuildPersonalizedPageRankRecords \
   -input data/p2p-Gnutella08-adj.txt -output PageRankRecords \
   -numNodes 6301 -sources 123,456,789
```

Partition the graph using hash partitioning

```bash
hadoop fs -mkdir PageRank;

hadoop jar target/assignments-1.0.jar \
   coursework.PageRank.PartitionGraph \
   -input PageRankRecords \
   -output PageRank/iter0000 -numPartitions 5 -numNodes 6301
```

Run the main driver and iterate multi-source PageRank

```bash
hadoop jar target/assignments-1.0.jar \
   coursework.PageRank.RunPersonalizedPageRankBasic \
   -base PageRank -numNodes 6301 -start 0 -end 20 \
   -sources 123,456,789
```

Extract the top 10 personalized PageRank values

```bash
hadoop jar target/assignments-1.0.jar \
   coursework.PageRank.FindMaxPageRankNodes \
   -input PageRank/iter0020 -output PageRank-top10 \
   -top 10
```


## Coursework 6: SQL Data Analytics

**Directory:** `src/main/scala/coursework/SQLAnalytics`

This project implements a series of SQL queries in Spark, operating directly with RDDs instead of using the DataFrame API or Spark SQL. It essentially bridges the gap between raw SQL queries and Spark's execution framework. The data used for this project is sourced from the [TPC-H benchmark](https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-H_v3.0.1.pdf). Our programs are designed to handle both plain-text data and Parquet files.

A key aspect of this implementation is the emphasis on efficiency, especially in the way transformations and table joins are conducted. For instance, we prioritize hash joins and map-side joins over reduce-side `cogroup` to optimize performance.

Below are a few example queries.

**Query 2**: Identifying clerks responsible for processing items shipped on a specific date and listing the first 20 by order key.

```sql
select o_clerk, o_orderkey from lineitem, orders
where
  l_orderkey = o_orderkey and
  l_shipdate = 'YYYY-MM-DD'
order by o_orderkey asc limit 20;
```


**Query 5**: Comparing shipments to Canada and the United States by month, using all available data.

```sql
select n_nationkey, n_name, year-month, count(*) from lineitem, orders, customer, nation
where
   l_orderkey = o_orderkey and
   o_custkey = c_custkey and
   c_nationkey = n_nationkey and
   c_nationkey IN ('CANADA', 'UNITED STATES')
group by n_nationkey, n_name, year-month
order by n_nationkey, year-month asc;
```

**Query 6**: Reporting the volume of business in terms of billing, shipping, and returns.

```sql
select
  l_returnflag,
  l_linestatus,
  sum(l_quantity) as sum_qty,
  sum(l_extendedprice) as sum_base_price,
  sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
  sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
  avg(l_quantity) as avg_qty,
  avg(l_extendedprice) as avg_price,
  avg(l_discount) as avg_disc,
  count(*) as count_order
from lineitem
where
  l_shipdate = 'YYYY-MM-DD'
group by l_returnflag, l_linestatus;
```

**Query 7**: Retrieving the top 5 unshipped orders with the highest value.

```sql
select
  c_name,
  l_orderkey,
  sum(l_extendedprice*(1-l_discount)) as revenue,
  o_orderdate,
  o_shippriority
from customer, orders, lineitem
where
  c_custkey = o_custkey and
  l_orderkey = o_orderkey and
  o_orderdate < "YYYY-MM-DD" and
  l_shipdate > "YYYY-MM-DD"
group by
  c_name,
  l_orderkey,
  o_orderdate,
  o_shippriority
order by
  revenue desc
limit 5;
```


## Coursework 7: Spark Streaming

**Directory:** `src/main/scala/coursework/streaming`

This project leverages Spark Streaming to analyze taxi trip data, focusing on specific locations and trends in trip arrivals.

**RegionEventCount.scala**
Processes streaming taxi trip data to calculate the hourly number of trips that end at either the Goldman Sachs headquarters or the Citigroup headquarters. 

**TrendingArrivals.scala**
Functioning as a trend detector, this part identifies periods with a high number of arrivals at the Goldman Sachs or Citigroup headquarters. It examines data in ten-minute intervals (e.g., 6:00 to 6:10, 6:10 to 6:20, etc.). The system triggers an alert when the current interval's arrivals are at least double those of the previous interval, with a caveat to avoid false alarms: it only activates if there are ten or more arrivals in the current interval. This feature helps filter out less significant fluctuations, focusing on genuinely notable trends.

**Execution commands:**

```bash
spark-submit --class coursework.streaming.RegionEventCount \
   target/assignments-1.0.jar --input taxi-data --checkpoint checkpoint --output output
```

```bash
spark-submit --class coursework.streaming.TrendingArrivals \
   target/assignments-1.0.jar --input taxi-data --checkpoint checkpoint --output output &> output.log
```

