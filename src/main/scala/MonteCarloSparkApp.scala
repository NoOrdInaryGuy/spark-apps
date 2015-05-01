import org.apache.spark.{SparkContext, SparkConf}
import scala.math.random

object MonteCarloSparkApp {
  val numTrials = 100000000
  val parallelism = 8

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Monte Carlo Spark App")
    val sc = new SparkContext(conf)

    val pointsInCircleRdd = sc.parallelize(1 to numTrials, parallelism)
    val pointsInCircle = pointsInCircleRdd.filter {
      _ => Math.pow(random * 2 - 1, 2) + Math.pow(random * 2 - 1, 2) < 1
    }.count()

    val pi = pointsInCircle * 4.0 / numTrials
    println(s"Pi is: $pi")
  }
}
