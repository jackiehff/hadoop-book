import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class MaxTemperatureSpark {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperatureSpark <input path> <output path>");
            System.exit(-1);
        }

        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext("local", "MaxTemperatureSpark", conf);
        JavaRDD<String> lines = sc.textFile(args[0]);
        JavaRDD<String[]> records = lines.map((Function<String, String[]>) s -> s.split("\t"));
        JavaRDD<String[]> filtered = records.filter((Function<String[], Boolean>) rec -> !rec[1].equals("9999") && rec[2].matches("[01459]"));
        JavaPairRDD<Integer, Integer> tuples = filtered.mapToPair(
                (PairFunction<String[], Integer, Integer>) rec -> new Tuple2<>(
                        Integer.parseInt(rec[0]), Integer.parseInt(rec[1]))
        );
        JavaPairRDD<Integer, Integer> maxTemps = tuples.reduceByKey(
                (Function2<Integer, Integer, Integer>) Math::max
        );
        maxTemps.saveAsTextFile(args[1]);
    }
}
