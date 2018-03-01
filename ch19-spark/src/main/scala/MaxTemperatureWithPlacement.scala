import org.apache.spark.{SparkConf, SparkContext}

object MaxTemperatureWithPlacement {

  def main(args: Array[String]): Unit = {
    val inputPath = args(0)
    val conf = new SparkConf().setAppName("Max Temperature")
    //val preferredLocations = InputFormatInfo.computePreferredLocations(
    //  Seq(new InputFormatInfo(new Configuration(), classOf[TextInputFormat],
    //    inputPath)))
    //val sc = new SparkContext(conf, preferredLocations)
    val sc = new SparkContext(conf)

    sc.textFile(inputPath)
      .map(_.split("\t"))
      .filter(rec => rec(1) != "9999" && rec(2).matches("[01459]"))
      .map(rec => (rec(0).toInt, rec(1).toInt))
      .reduceByKey((a, b) => Math.max(a, b))
      .saveAsTextFile(args(1))

    sc.stop()
  }
}