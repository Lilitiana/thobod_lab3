import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MainTest {
    static JavaRDD<String> file;
    @BeforeAll
    public static void Init() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);
        file = sc.textFile("D:\\Для учёбы\\4 курс\\БигДата\\1");
    }
    @Test
    void calcBrowsersCountTest() {
        Map<String,Integer> map= Main.CalcBrowsersCount(file);
        assertEquals(map.get("Opera"),253);
    }

    @Test
    void MRTaskTest() {
        Map<String,Item> map=Main.MRTask(file);
        Item testArr=map.get("ip1");
        assertEquals(testArr.Sum,2762635);
    }
}