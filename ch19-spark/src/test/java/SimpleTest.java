import com.google.common.io.Files;
import com.google.common.io.Resources;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;

import static org.junit.Assert.assertEquals;

public class SimpleTest implements Serializable { // needs to be serializable for the Fn

    @Rule
    public transient TemporaryFolder tmpDir = new TemporaryFolder();

    @Test
    public void test() throws IOException, URISyntaxException {
        File inputFile = tmpDir.newFile("input");
        //Files.copy(Resources.newInputStreamSupplier(Resources.getResource("fruit.txt")), inputFile);
        Files.copy(new File(Resources.getResource("fruit.txt").toURI()), inputFile);
        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext("local", "Simple App", conf);
        JavaRDD<String> a = sc.textFile(inputFile.getPath()).cache();

        dump(a);

        long numEven = a.filter((Function<String, Boolean>) input -> {
            return input.length() % 2 == 0; // even
        }).count();

        assertEquals(2, numEven);

        sc.stop();
    }

    <T> String dump(JavaRDD<T> rdd) {
        final StringBuilder sb = new StringBuilder("{");
        for (T t : rdd.collect()) {
            sb.append(t).append(",");
        }
        if (sb.length() > 1) {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append("}");
        return sb.toString();
    }

    <K, V> String dump(JavaPairRDD<K, V> rdd) {
        final StringBuilder sb = new StringBuilder("{");
        for (Tuple2<K, V> pair : rdd.collect()) {
            sb.append("(").append(pair._1()).append(",").append(pair._2()).append(")").append(",");
        }
        if (sb.length() > 1) {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append("}");
        return sb.toString();
    }
}
