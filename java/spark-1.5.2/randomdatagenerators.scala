package sparklyr

import org.apache.spark.mllib.random.RandomDataGenerator
import org.apache.commons.math3.distribution._

trait RealDistributionGenerator extends RandomDataGenerator[Double] {
  protected val dist: AbstractRealDistribution

  override def nextValue(): Double = dist.sample

  override def setSeed(seed: Long): Unit = {
    dist.reseedRandomGenerator(seed)
  }
}

trait IntegerDistributionGenerator extends RandomDataGenerator[Int] {
  protected val dist: AbstractIntegerDistribution

  override def nextValue(): Int = dist.sample

  override def setSeed(seed: Long): Unit = {
    dist.reseedRandomGenerator(seed)
  }
}

class BetaGenerator(val alpha: Double, val beta: Double) extends RealDistributionGenerator {
  override val dist = new BetaDistribution(alpha, beta)

  override def copy(): BetaGenerator = new BetaGenerator(alpha, beta)
}

class BinomialGenerator(val trials: Int, val p: Double) extends IntegerDistributionGenerator {
  override val dist = new BinomialDistribution(trials, p)

  override def copy(): BinomialGenerator = new BinomialGenerator(trials, p)
}

class NormalGenerator(val mean: Double, val sd: Double) extends RealDistributionGenerator {
  override val dist = new NormalDistribution(mean, sd)

  override def copy(): NormalGenerator = new NormalGenerator(mean, sd)
}

class UniformGenerator(val lower: Double, val upper: Double) extends RealDistributionGenerator {
  override val dist = new UniformRealDistribution(lower, upper)

  override def copy(): UniformGenerator = new UniformGenerator(lower, upper)
}
