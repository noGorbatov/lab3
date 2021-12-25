package ru.bmstu.spark.lab3;

import java.io.Serializable;

public class ParsedData implements Serializable {
    private int srcAirport;
    private int destAirport;
    private int delayTime;
    private boolean delayed;
    private boolean cancelled;
    private static String SPACE_SEP = ",";
    private static int SRC_AIRPORT_POS = 11;
    private static int DEST_AIRPORT_POS = 14;
    private static int DELAY_TIME_POS = 18;
    private static int CANCELLED_POS = 19;

    public static ParsedData parse(String line) {
        String[] data = line.split(SPACE_SEP);
        ParsedData res = new ParsedData();
        res.srcAirport = Integer.parseInt(data[SRC_AIRPORT_POS]);
        res.destAirport = Integer.parseInt(data[DEST_AIRPORT_POS]);
        res.cancelled = data[CANCELLED_POS].equals("1");
        if (res.cancelled) {
            return res;
        }
        res.delayTime = Integer.parseInt(data[DELAY_TIME_POS]);
        res.delayed = res.delayTime > 0;
        return res;
    }

    public int getSrcAirport() {
        return srcAirport;
    }

    public int getDestAirport() {
        return destAirport;
    }

    public int getDelayTime() {
        return delayTime;
    }

    public boolean getDelayed() {
        return delayed;
    }

    public boolean getCancelled() {
        return cancelled;
    }

}
