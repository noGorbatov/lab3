package ru.bmstu.spark.lab3;

import java.io.Serializable;

public class ParsedData implements Serializable {
    private int srcAirport;
    private int destAirport;
    private int delayTime;
    private boolean delayed;
    private boolean cancelled;
    private static String SPACE_SEP = " ";
    private static int SRC_AIRPORT_POS = 
    public static ParsedData parse(String line) {
        String[] data = line.split(SPACE_SEP);

    }
}
