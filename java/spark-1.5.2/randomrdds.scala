package sparklyr

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object RandomRDDs {
  def betaRDD(
      sc: SparkContext,
      alpha: Double,
      beta: Double,
      size: Long,
      numPartitions: Int,
      seed: Long): RDD[Double] = {
    val generator = new BetaGenerator(alpha, beta)
    org.apache.spark.mllib.random.RandomRDDs.randomRDD(
      sc, generator, size, numPartitions, seed
    )
  }

  def binomialRDD(
      sc: SparkContext,
      trials: Int,
      p: Double,
      size: Long,
      numPartitions: Int,
      seed: Long): RDD[Int] = {
    val generator = new BinomialGenerator(trials, p)
    org.apache.spark.mllib.random.RandomRDDs.randomRDD(
      sc, generator, size, numPartitions, seed
    )
  }

  def normalRDD(
      sc: SparkContext,
      mean: Double,
      sd: Double,
      size: Long,
      numPartitions: Int,
      seed: Long): RDD[Double] = {
    val generator = new NormalGenerator(mean, sd)
    org.apache.spark.mllib.random.RandomRDDs.randomRDD(
      sc, generator, size, numPartitions, seed
    )
  }

  def uniformRDD(
      sc: SparkContext,
      min: Double,
      max: Double,
      size: Long,
      numPartitions: Int,
      seed: Long): RDD[Double] = {
    val generator = new UniformGenerator(min, max)
    org.apache.spark.mllib.random.RandomRDDs.randomRDD(
      sc, generator, size, numPartitions, seed
    )
  }
}
