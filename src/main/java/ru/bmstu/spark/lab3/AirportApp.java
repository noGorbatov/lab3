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
    private static final String KEY_NAME_SEP = ",\"";
    private static final int KEY_POS = 0;
    private static final int NAME_POS = 1;

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> ids = sc.textFile("/ids.csv");

        JavaRDD<String> filteredIds = ids.filter( line -> !Character.isAlphabetic(line.charAt(0)) );
        JavaPairRDD<Integer, String> airportMapRdd = filteredIds.mapToPair(
                line -> {
                    String[] pair = line.split(KEY_NAME_SEP);
                    int key = Integer.parseInt(pair[KEY_POS].replaceAll("\"", ""));
                    String name = pair[NAME_POS].replaceAll("\"", "");
                    return new Tuple2<>(key, name);
        });

        final Broadcast<Map<Integer, String>> airportsBroadcasted = sc.broadcast(airportMapRdd.collectAsMap());

        JavaRDD<String> stats = sc.textFile("/stats.csv");
        JavaRDD<String> filteredStats = stats.filter( line -> Character.isDigit(line.charAt(0)) );
        JavaPairRDD<AirportKey, FlightData> statsRdd = filteredStats.mapToPair(line -> {
                    ParsedData parsedData = ParsedData.parse(line);
                    return new Tuple2<>(new AirportKey(parsedData.getSrcAirport(), parsedData.getDestAirport()),
                                        new FlightData(parsedData.getDelayTime(), parsedData.getDelayed(),
                                                        parsedData.getCancelled()));
                }
        );

//        JavaPairRDD<AirportKey, FlightData> airportStats = statsRdd.reduceByKey(
//                (flightAcc, flightData) -> flightAcc.add(flightData)
//        );
        JavaPairRDD<AirportKey, Iterable<FlightData>> airportGrouped = statsRdd.groupByKey();
        JavaPairRDD<AirportKey, FlightData> airportStats = airportGrouped.mapToPair(
                entry -> {
                    AirportKey key = entry._1;
                    Iterable<FlightData> group = entry._2;
                    FlightData res = new FlightData();
                    for (FlightData data: group) {
                        res = res.add(data);
                    }
                    return new Tuple2<>(key, res);
                }
        );

        JavaRDD<String> resultStats = airportStats.map(
                (entry) -> {
                    AirportKey key = entry._1;
                    FlightData data = entry._2;
                    Map<Integer, String> airportsInfo = airportsBroadcasted.value();
                    String srcAirport = airportsInfo.get(key.getSrcAirport());
                    String destAirport = airportsInfo.get(key.getDestAirport());
                    return "Flights stats:\nfrom " + srcAirport + "\nto " + destAirport +
                            "\nmax delay time = " + data.getMaxDelayTime() + "\ndelayed flights = " +
                            data.getDelayedPercent() + "%\ncancelled flights = " +
                            data.getCancelledPercent() + "%\n\n";
                }
        );

        resultStats.saveAsTextFile("results");

    }
}
