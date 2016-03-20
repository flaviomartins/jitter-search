package io.jitter.core.document;

import java.util.LinkedHashMap;

public class DocVector {

    public LinkedHashMap<String, Integer> vector;

    public DocVector() {
        this.vector = new LinkedHashMap<>();
    }

    public void put(String term, int freq) {
        vector.put(term, freq);
    }

}
