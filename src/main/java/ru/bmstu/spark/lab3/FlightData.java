package ru.bmstu.spark.lab3;

import java.io.Serializable;

public class FlightData implements Serializable {
    private int delayTime;
    private boolean delayed;
    private boolean cancelled;
    public FlightData(int delay, boolean delayed, boolean cancelled) {
        
    }
}
