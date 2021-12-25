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
    private static final int PERCENT_MULT = 100;
    public FlightData(float delayTime, boolean delayed, boolean cancelled) {
        this.delayTime = delayTime;
        this.delayed = delayed;
        this.cancelled = cancelled;
    }

    public FlightData() {}

    public float getMaxDelayTime() {
        return maxDelayTime;
    }

    public int getDelayedPercent() {
        if (flightNumber == 0) return -1;
        float res = (float)delayedNumber / flightNumber * PERCENT_MULT;
        return (int) res;
    }

    public float getCancelledPercent() {
        if (flightNumber == 0) return -1;
        float res = (float)cancelledNumber / flightNumber * PERCENT_MULT;
        return res;
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
