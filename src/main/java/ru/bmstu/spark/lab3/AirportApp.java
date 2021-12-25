package ru.bmstu.spark.lab3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class AirportApp {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> stats = sc.textFile("/ids.csv");
        List<String> res = stats.collect();
        int i = 0;
        for (String line: res) {
            System.out.println(line);
            i++;
            if (i > 5) break;
        }

        JavaRDD<String> s
    }
}
