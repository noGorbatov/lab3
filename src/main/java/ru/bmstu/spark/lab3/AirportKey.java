package ru.bmstu.spark.lab3;

import java.io.Serializable;

public class AirportKey implements Serializable {
    private int srcAirport;
    private int destAirport;
    public AirportKey(int src, int dest) {
        srcAirport = src;
        destAirport = dest;
    }

    public int getDestAirport() {
        return destAirport;
    }

    public int getSrcAirport() {
        return srcAirport;
    }
}
