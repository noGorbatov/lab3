package ru.bmstu.spark.lab3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

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
        JavaPairRDD<Tuple2<Integer, Integer>, FlightData> statsRdd = filteredStats.mapToPair(line -> {
                    ParsedData parsedData = ParsedData.parse(line);
                    return new Tuple2<>(new Tuple2<>(parsedData.getSrcAirport(), parsedData.getDestAirport()),
                                        new FlightData(parsedData.getDelayTime(), parsedData.getDelayed(),
                                                        parsedData.getCancelled()));
                }
        );

        /*JavaPairRDD<Tuple2<Integer, Integer>, FlightData> airportStats = statsRdd.reduceByKey(
                (flightAcc, flightData) -> flightAcc.add2(flightData)
        );*/
//        JavaPairRDD<Tuple2<Integer, Integer>, FlightData> airportStats = statsRdd.groupByKey().mapToPair(
//                entry -> {
//                    Tuple2<Integer, Integer> key = entry._1;
//                    Iterable<FlightData> group = entry._2;
//                    FlightData res = new FlightData();
//                    for (FlightData data: group) {
//                        res = res.add(data);
//                    }
//                    return new Tuple2<>(key, res);
//                }
//        );
        JavaPairRDD<Tuple2<Integer, Integer>, FlightData> airportStats = statsRdd.combineByKey(
                fData -> new FlightData(fData),
                (combiner, fData) -> combiner.add(fData)

        )

        JavaRDD<String> resultStats = airportStats.map(
                (entry) -> {
                    Tuple2<Integer, Integer> key = entry._1;
                    int srcAirportInt = key._1;
                    int destAirportInt = key._2;
                    FlightData data = entry._2;
                    Map<Integer, String> airportsInfo = airportsBroadcasted.value();
                    String srcAirport = airportsInfo.get(srcAirportInt);
                    String destAirport = airportsInfo.get(destAirportInt);
                    return "Flights stats:\nfrom " + srcAirport + "\nto " + destAirport +
                            "\nmax delay time = " + data.getMaxDelayTime() + "\ndelayed flights = " +
                            data.getDelayedPercent() + "%\ncancelled flights = " +
                            data.getCancelledPercent() + "%\n\n";
                }
        );

        resultStats.saveAsTextFile("results");

    }
}
