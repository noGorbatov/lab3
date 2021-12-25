package ru.bmstu.spark.lab3;

import java.io.Serializable;

public class FlightData implements Serializable {
    private float delayTime = 0;
    private boolean delayed = false;
    private boolean cancelled = false;
    private float maxDelayTime = 0;
    private int flightNumber = 0;
    private int delayedNumber = 0;
    private int cancelledNumber = 0;
    public FlightData(float delayTime, boolean delayed, boolean cancelled) {
        this.delayTime = delayTime;
        this.delayed = delayed;
        this.cancelled = cancelled;
    }

    public float getMaxDelayTime() {
        return maxDelayTime;
    }

    public float getDelayedPercent() {
        return 
    }

    @Override
    public String toString() {
        return "FlightData{" +
                "delayTime=" + delayTime +
                ", delayed=" + delayed +
                ", cancelled=" + cancelled +
                '}';
    }

    public FlightData add(FlightData data) {
        flightNumber++;
        if (data.cancelled) {
            cancelledNumber++;
            return this;
        }
        if (data.delayed) {
            delayedNumber++;
            if (data.delayTime > maxDelayTime) {
                maxDelayTime = data.delayTime;
            }
        }
        return this;
    }
}
