package org.novasearch.jitter.core.selection.methods;

import org.novasearch.jitter.api.search.Document;

import java.util.*;

public abstract class SelectionMethod {

    protected SelectionMethod() {
    }

    public Map<String, Float> getCounts(List<Document> results) {
        Map<String, Float> counts = new HashMap<>();
        for (Document result : results) {
            String screenName = result.getScreen_name();
            if (!counts.containsKey(screenName)) {
                counts.put(screenName, 1f);
            } else {
                float cur = counts.get(screenName);
                counts.put(screenName, cur + 1f);
            }
        }
        return counts;
    }

    public Map<String, Float> getRanked(List<Document> results) {
        Map<String, Float> map = rank(results);
        return map;
    }


    public abstract Map<String, Float> rank(List<Document> results);

}
