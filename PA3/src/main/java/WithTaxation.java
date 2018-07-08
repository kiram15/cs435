import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

public final class WithTaxation {

    private static final Pattern COLON = Pattern.compile(":");
    public static AtomicInteger lineCount = new AtomicInteger();

    private static class Sum implements Function2<Double, Double, Double> {
        @Override
        public Double call(Double a, Double b) {
            return a + b;
        }
    }

    private static class Add {
        public Tuple2<String, String> increment (String s) {
            String count = Integer.toString(lineCount.incrementAndGet());
            Tuple2<String, String> t2 = new Tuple2<>(count, s);
            return t2;
        }
    }

    public static void main(String[] argv){
        if (argv.length != 3) {
            System.err.printf("Usage: %s [generic options] <input> <output> <titles>\n",
                    Idealized.class.getSimpleName());
            return;
        }

        String inputPath = argv[0];
        String outputPath = argv[1];
        String titles = argv[2];

//        SparkSession spark = SparkSession
//                .builder()
//                .appName("JavaPageRank")
//                .getOrCreate();

        SparkConf conf = new SparkConf().setAppName("Idealized").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> file = sc.textFile(inputPath);

        // Loads all URLs from input file and initialize their neighbors.
        JavaPairRDD<String, String> links = file.mapToPair(s -> {
            String[] parts = COLON.split(s);
            return new Tuple2<>(parts[0], parts[1].trim());
        }).distinct().cache();

        JavaPairRDD<String, Double> ranks = links.mapValues(rs -> 1.0);

        // Calculates and updates URL ranks continuously using PageRank algorithm.
        for (int current = 0; current < 25; current++) {
            // Calculates URL contributions to the rank of other URLs.
            JavaPairRDD<String, Double> contribs = links.join(ranks).values()
                    .flatMapToPair(s -> {
                        String[] splitVals = s._1.split("\\s+");
                        int urlCount = splitVals.length;

                        List<Tuple2<String, Double>> results = new ArrayList<>();
                        for (String n: splitVals) {
                            results.add(new Tuple2<>(n, s._2() / urlCount));
                        }
                        return results.iterator();
                    });
            ranks = contribs.reduceByKey(new Sum()).mapValues(sum -> 0.15 + sum * 0.85);
        }

        JavaRDD<String> allTitles = sc.textFile(titles);

        //return string of int, string of title
        JavaPairRDD<String, String> lineTitles = allTitles.mapToPair(s -> {
            Add a = new Add();
            return a.increment(s);
        }).distinct().cache();

        //joined = String (key/line num), Tuple2<PR, title>
        JavaPairRDD<String, Tuple2<String, Double>> joined = lineTitles.join(ranks);

        //isolate tuple to sort by key
        JavaPairRDD<Double,String> r =
                joined.values().mapToPair((t)->new Tuple2(
                        t._2,
                        t._1));

        //sort in decending order
        r.sortByKey(false).saveAsTextFile(outputPath);
        //spark.stop();
    }
}
