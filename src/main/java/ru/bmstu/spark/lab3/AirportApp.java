package ru.bmstu.spark.lab3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class AirportApp {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> ids = sc.textFile("/ids.csv");
        List<String> res = ids.collect();
        int i = 0;
        for (String line: res) {
            System.out.println(line);
            i++;
            if (i > 5) break;
        }

        JavaRDD<String> stats = sc.textFile("/stats.csv");
        JavaPairRDD<AirportKey, FlightData> statsRdd = stats.mapToPair(line -> return )
    }
}
