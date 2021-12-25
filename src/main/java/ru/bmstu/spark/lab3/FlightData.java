package ru.bmstu.spark.lab3;

import java.io.Serializable;

public class FlightData implements Serializable {
    private float delayTime;
    private boolean delayed;
    private boolean cancelled;
    public FlightData(float delayTime, boolean delayed, boolean cancelled) {
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
