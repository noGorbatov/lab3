package ru.bmstu.spark.lab3;

import java.io.Serializable;
import java.util.Objects;

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

    @Override
    public String toString() {
        return "AirportKey{" +
                "srcAirport=" + srcAirport +
                ", destAirport=" + destAirport +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AirportKey that = (AirportKey) o;
        return srcAirport == that.srcAirport && destAirport == that.destAirport;
    }

    @Override
    public int hashCode() {
        return Objects.hash(srcAirport, destAirport);
    }
}
