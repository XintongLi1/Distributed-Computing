# Distributed-Computing
MapReduce, Spark, Inverted texting and Search, Graph Algorithms, Data Mining and ML, Real-time Analytics, etc

All projects are conveniently built using the `mvn clean package` command.


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
