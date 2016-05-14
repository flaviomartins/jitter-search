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
        for (String term : vector.keySet()) {
            norm += Math.pow(vector.get(term), 2.0);
        }
        return Math.sqrt(norm);
    }

    public double computeL1Norm() {
        double norm = 0.0;
        for (String term : vector.keySet()) {
            norm += Math.abs(vector.get(term));
        }
        return norm;
    }
}
