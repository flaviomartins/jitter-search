package io.jitter.core.selection.methods;

import io.jitter.api.search.Document;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CRCSLOGISR extends CRCS {

    CRCSLOGISR() {
    }

    @Override
    public Map<String, Double> rank(List<Document> results) {
        HashMap<String, Double> scores = getScores(results);
        Map<String, Double> counts = getCounts(results);

        // log multiplication
        for (Map.Entry<String, Double> entry : scores.entrySet()) {
            double count = counts.get(entry.getKey());
            scores.put(entry.getKey(), Math.log(1.0 + count) * entry.getValue());
        }
        return scores;
    }

    double weight(int j, int size) {
        return 1.0 / Math.pow(j, 2);
    }

}
