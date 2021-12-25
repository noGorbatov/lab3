package ru.bmstu.spark.lab3;

import java.io.Serializable;

public class ParsedData implements Serializable {
    private int srcAirport;
    private int destAirport;
    private int delayTime;
    private boolean delayed;
    private boolean cancelled;
    public static ParsedData parse(String line) {
        
    }
}
