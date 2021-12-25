package ru.bmstu.spark.lab3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

public class AirportApp {
    private static String COMMA_SEP = ",";
    private static int KEY_POS = 0;
    private static int NAME_POS = 0;

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

        JavaRDD<String> filteredIds = ids.filter( line -> !Character.isAlphabetic(line.charAt(0)) );
        JavaPairRDD<Integer, String> airportMapRdd = filteredIds.mapToPair(
                line -> {
                    String[] pair = line.split(COMMA_SEP);
                    int key = Integer.parseInt(pair[KEY_POS].replaceAll("\"", ""));
                    String name = pair[NAME_POS].replaceAll("\"", "");
                    return new Tuple2<>(key, name);
        });

        final Broadcast<Map<Integer, String>> airportsBroadcasted = sc.broadcast(airportMapRdd.collectAsMap());

        JavaRDD<String> stats = sc.textFile("/stats.csv");
        System.out.println("unfiltered records " + stats.count());
        JavaRDD<String> filteredStats = stats.filter( line -> Character.isDigit(line.charAt(0)) );
        System.out.println("filtered records " + filteredStats.count());
        JavaPairRDD<AirportKey, FlightData> statsRdd = filteredStats.mapToPair(line -> {
                    ParsedData parsedData = ParsedData.parse(line);
                    return new Tuple2<>(new AirportKey(parsedData.getSrcAirport(), parsedData.getDestAirport()),
                                        new FlightData(parsedData.getDelayTime(), parsedData.getDelayed(),
                                                        parsedData.getCancelled()));
                }
        );

        statsRdd.take(3).forEach(obj -> System.out.println(obj._1 + "\n" + obj._2 + "\n"));

        JavaPairRDD<AirportKey, FlightData> airportStats = statsRdd.reduceByKey(
                FlightData::add);

        JavaRDD<String> resultStats = airportStats.map(
                (entry) -> {
                    AirportKey key = entry._1;
                    FlightData data = entry._2;
                    Map<Integer, String> airportsInfo = airportsBroadcasted.value();
                    String srcAirport = airportsInfo.get(key.getSrcAirport());
                    String destAirport = airportsInfo.get(key.getDestAirport());
                    return "Flights stats from " +
                }
        );

    }
}
