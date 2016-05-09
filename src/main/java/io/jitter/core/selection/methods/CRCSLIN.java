package io.jitter.core.selection.methods;

import io.jitter.api.search.Document;

import java.util.List;
import java.util.Map;

public class CRCSLIN extends CRCS {

    CRCSLIN() {
    }

    @Override
    public Map<String, Double> rank(List<Document> results) {
        return getScores(results);
    }

    double weight(int j, int size) {
        return size - j;
    }
}
