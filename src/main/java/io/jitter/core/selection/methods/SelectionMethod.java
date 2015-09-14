package io.jitter.core.selection.methods;

import io.jitter.api.search.Document;
import io.jitter.core.selection.ShardStats;

import java.util.*;

public abstract class SelectionMethod {

    SelectionMethod() {
    }

    Map<String, Double> getCounts(List<Document> results) {
        Map<String, Double> counts = new HashMap<>();
        for (Document result : results) {
            String screenName = result.getScreen_name();
            if (!counts.containsKey(screenName)) {
                counts.put(screenName, 1d);
            } else {
                double cur = counts.get(screenName);
                counts.put(screenName, cur + 1d);
            }
        }
        return counts;
    }

    public abstract Map<String, Double> rank(List<Document> results);

    public Map<String, Double> normalize(Map<String, Double> rank, ShardStats shardStats) {
        return rank;
    }

}
