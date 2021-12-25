package ru.bmstu.spark.lab3;

import java.io.Serializable;

public class ParsedData implements Serializable {
    private int srcAirport;
    private int destAirport;
    private float delayTime;
    private boolean delayed;
    private boolean cancelled;
    private static final String SPACE_SEP = ",";
    private static final int SRC_AIRPORT_POS = 11;
    private static final int DEST_AIRPORT_POS = 14;
    private static final int DELAY_TIME_POS = 18;
    private static final int CANCELLED_POS = 19;
    private static final String CANCELLED_VALUE = "1.00";

    public static ParsedData parse(String line) {
        String[] data = line.split(SPACE_SEP);
        ParsedData res = new ParsedData();
        res.srcAirport = Integer.parseInt(data[SRC_AIRPORT_POS]);
        res.destAirport = Integer.parseInt(data[DEST_AIRPORT_POS]);
        res.cancelled = data[CANCELLED_POS].equals(CANCELLED_VALUE);
        if (res.cancelled) {
            return res;

        }
        if (data[DELAY_TIME_POS].equals("")) {
            return res;
        }
        res.delayTime = Float.parseFloat(data[DELAY_TIME_POS]);
        res.delayed = res.delayTime > 0;
        return res;
    }

    public int getSrcAirport() {
        return srcAirport;
    }

    public int getDestAirport() {
        return destAirport;
    }

    public float getDelayTime() {
        return delayTime;
    }

    public boolean getDelayed() {
        return delayed;
    }

    public boolean getCancelled() {
        return cancelled;
    }

}
