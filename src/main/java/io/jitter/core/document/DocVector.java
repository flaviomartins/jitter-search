package io.jitter.core.document;

import java.util.LinkedHashMap;

public class DocVector {

    public final LinkedHashMap<String, Integer> vector;

    public DocVector() {
        this.vector = new LinkedHashMap<>();
    }

    public int getTermFreq(String term) {
        Integer w = vector.get(term);
        return (w == null) ? 0 : w;
    }

    public void setTermFreq(String term, int freq) {
        vector.put(term, freq);
    }

    public double getLength() {
        return computeL1Norm();
    }

    public double computeL2Norm() {
        double norm = 0.0;
        for (Integer i : vector.values()) {
            norm += Math.pow(i, 2.0);
        }
        return Math.sqrt(norm);
    }

    public double computeL1Norm() {
        double norm = 0.0;
        for (Integer i : vector.values()) {
            norm += Math.abs(i);
        }
        return norm;
    }
}
