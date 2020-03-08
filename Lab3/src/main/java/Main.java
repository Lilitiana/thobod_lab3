import eu.bitwalker.useragentutils.UserAgent;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class Main {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> strings = sc.textFile("D:\\Дляучёбы\\4курс\\БигДата\\1.txt");

        CalcBrowsersCount (strings);
        MRTask(strings);

        System.out.println("Готово");
    }
    public static Map<String,Integer> CalcBrowsersCount(JavaRDD<String> arr){

        Map<String,Integer> counts=arr
                .mapToPair(p->{
                    UserAgent userAgent = UserAgent.parseUserAgentString(p);
                    String browser = userAgent.getBrowser().getGroup().getName();
                    return new Tuple2<String,Integer>(browser, 1);
                }).reduceByKey((p,c)->p+c)
                .collectAsMap();

        counts.forEach((p,c)->System.out.println(p+" : "+c));
        return counts;
    }
    public static Map<String,Item> MRTask(JavaRDD<String> arr) {

        JavaPairRDD<String,Item> items=arr
                .mapToPair(p->{
                    UserAgent userAgent = UserAgent.parseUserAgentString(p);
                    String ip = p.split(" ")[0];
                    int size = 0;

                    try {
                        size = Integer.parseInt(p.split(" ")[9]);
                    } catch (Exception ex) {
                    }
                    return new Tuple2<String,Item>(ip,new Item(1,size));
                }).reduceByKey((p,c)->new Item(p.Count+c.Count,p.Sum+c.Sum))
                .sortByKey();
        items.saveAsTextFile("D:\\Дляучёбы\\4курс\\БигДата\\reduse.csv");
        return items.collectAsMap();
    }
}