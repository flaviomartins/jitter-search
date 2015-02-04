package org.novasearch.jitter.core.selection;

import org.novasearch.jitter.api.search.Document;

import java.util.*;

public abstract class SelectionMethod {

    protected SelectionMethod() {
    }

    public Map<String, Float> getCounts(List<Document> results) {
        HashMap<String, Float> counts = new HashMap<>();
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

    public SortedMap<String, Float> getRanked(List<Document> results) {
        Map<String, Float> map = rank(results);
        return getSortedMap(map);
    }

    private SortedMap<String, Float> getSortedMap(Map<String, Float> map) {
        SelectionComparator comparator = new SelectionComparator(map);
        TreeMap<String, Float> sortedMap = new TreeMap<>(comparator);
        sortedMap.putAll(map);
        return sortedMap;
    }

    public abstract Map<String, Float> rank(List<Document> results);

}
