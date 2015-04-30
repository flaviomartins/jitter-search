package org.novasearch.jitter.core.utils;

public class KeyValuePair implements Scorable {
    private String key;
    private double value;

    public KeyValuePair(String key, double value) {
        this.key = key;
        this.value = value;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    @Override
    public String toString() {
        return value + "\t" + key;
    }

    public void setScore(double score) {
        this.value = score;
    }

    public double getScore() {
        return value;
    }
}
