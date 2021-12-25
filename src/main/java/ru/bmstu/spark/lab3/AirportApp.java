package ru.bmstu.spark.lab3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

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
        System.out.println("unfiltered records " + stats.count());
        JavaRDD<String> filteredStats = stats.filter( s -> Character.isDigit(s.charAt(0)) );
        System.out.println("filtered records " + filteredStats.count());
        JavaPairRDD<AirportKey, FlightData> statsRdd = filteredStats.mapToPair(line -> {
                    ParsedData parsedData = ParsedData.parse(line);
                    return new Tuple2<>(new AirportKey(parsedData.getSrcAirport(), parsedData.getDestAirport()),
                                        new FlightData(parsedData.getDelayTime(), parsedData.getDelayed(),
                                                        parsedData.getCancelled()));
                }
        );

        statsRdd.take(3).forEach(obj -> System.out.println(obj._1 + "\n" + obj._2 + "\n"));

        JavaRDD<>
    }
}
