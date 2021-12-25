package ru.bmstu.spark.lab3;

import java.io.Serializable;

public class FlightData implements Serializable {
    private int delayTime;
    private boolean delayed;
    private boolean cancelled;
    public FlightData(int delayTime, boolean delayed, boolean cancelled) {
        this.delayTime = delayTime;
        this.delayed = delayed;
        this.cancelled = cancelled;
    }

    @Override
    public String toString() {
        return "FlightData{" +
                "delayTime=" + delayTime +
                ", delayed=" + delayed +
                ", cancelled=" + cancelled +
                '}';
    }
}
