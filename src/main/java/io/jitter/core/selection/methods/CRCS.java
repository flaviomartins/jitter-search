package io.jitter.core.selection.methods;

import io.jitter.api.search.Document;

import java.util.HashMap;
import java.util.List;

public abstract class CRCS extends SelectionMethod {

    CRCS() {
    }

    protected HashMap<String, Double> getScores(List<Document> results) {
        HashMap<String, Double> scores = new HashMap<>();
        int j = 1;
        for (Document result : results) {
            double r = weight(j, results.size());
            String screenName = result.getScreen_name();
            if (!scores.containsKey(screenName)) {
                scores.put(screenName, r);
            } else {
                double cur = scores.get(screenName);
                scores.put(screenName, cur + r);
            }
            j++;
        }
        return scores;
    }

    abstract double weight(int j, int size);

}
