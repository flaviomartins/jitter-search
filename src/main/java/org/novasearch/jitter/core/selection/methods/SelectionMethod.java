package org.novasearch.jitter.core.selection.methods;

import org.novasearch.jitter.api.search.Document;

import java.util.*;

public abstract class SelectionMethod {

    protected SelectionMethod() {
    }

    public Map<String, Double> getCounts(List<Document> results) {
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

    public Map<String, Double> getRanked(List<Document> results) {
        Map<String, Double> map = rank(results);
        return map;
    }


    public abstract Map<String, Double> rank(List<Document> results);

}
